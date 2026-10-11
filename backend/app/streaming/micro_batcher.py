"""ModelPulse Micro-Batching & Monitoring Orchestrator (Phase 5).
Accumulates streaming events into 5-minute event-time windows with:
- Configurable max batch size threshold.
- Duplicate event ID detection and idempotent processing.
- Separation of valid and quarantined records.
- Seamless invocation of Phase 3/4 MonitoringService and Severity Engine.
- Persistence error safety (no false durable-save claims).
"""
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple
import uuid
import pandas as pd

from backend.app.schemas.events import MicroBatchMetadata, TransactionEvent
from backend.app.schemas.monitoring import MonitoringRunRequest, MonitoringRunResponse
from backend.app.services.model_service import ModelService
from backend.app.services.monitoring_service import MonitoringService
from backend.app.streaming.metrics import StreamingMetrics, get_streaming_metrics

logger = logging.getLogger(__name__)


def align_to_window(dt: datetime, interval_seconds: int = 300) -> Tuple[datetime, datetime]:
    """Calculate event-time window bounds [window_start, window_end)."""
    # Ensure UTC timezone
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = dt.timestamp()
    start_ts = ts - (ts % interval_seconds)
    window_start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    window_end = window_start + timedelta(seconds=interval_seconds)
    return window_start, window_end


def make_deterministic_batch_id(
    window_start: datetime,
    window_end: datetime,
    source_offsets: Dict[str, int],
    events: Optional[List[Any]] = None,
    partition_ranges: Optional[Dict[str, Tuple[int, int]]] = None,
) -> str:
    """Generate a stable, deterministic, canonical micro-batch identifier for idempotent processing.
    Canonical batch fingerprint rules:
    - Stable time window coordinates [window_start, window_end) formatted in UTC ISO-8601.
    - Stable partition coordinates (offset ranges or max offsets per partition).
    - Canonical event digest: digests each event's id, timestamp, and features with sorted JSON keys.
      The event digests are sorted so that identical sets of events in any arrival order produce the
      EXACT SAME batch ID (order-invariant replay).
    - Altered payloads under the same event ID produce a DIFFERENT batch ID.
    - Partial batches (missing events) produce a DIFFERENT batch ID.
    """
    import hashlib
    import json

    w_start_str = window_start.strftime("%Y%m%dT%H%M%SZ")
    w_end_str = window_end.strftime("%Y%m%dT%H%M%SZ")

    if partition_ranges:
        coords_part = "_".join(
            f"p{p}:{min_o}-{max_o}" for p, (min_o, max_o) in sorted(partition_ranges.items())
        )
    else:
        coords_part = "_".join(f"p{p}o{o}" for p, o in sorted(source_offsets.items()))
    if not coords_part:
        coords_part = "empty"

    if events:
        event_hashes = []
        for e in events:
            if hasattr(e, "event_id"):
                eid = str(e.event_id)
                ts = str(e.timestamp.isoformat()) if hasattr(e, "timestamp") else ""
                feats = e.features if hasattr(e, "features") and isinstance(e.features, dict) else {}
            elif isinstance(e, dict):
                eid = str(e.get("event_id", ""))
                ts = str(e.get("timestamp", ""))
                feats = e.get("features", {}) if isinstance(e.get("features"), dict) else {}
            else:
                eid = str(e)
                ts = ""
                feats = {}

            canonical_repr = json.dumps(
                {"id": eid, "ts": ts, "features": feats},
                sort_keys=True,
                separators=(",", ":"),
            )
            h = hashlib.sha256(canonical_repr.encode("utf-8")).hexdigest()
            event_hashes.append(h)

        sorted_hashes = sorted(event_hashes)
        digest = hashlib.sha256("\n".join(sorted_hashes).encode("utf-8")).hexdigest()[:16]
        return f"mb-w{w_start_str}-{w_end_str}-{coords_part}-{digest}"

    return f"mb-w{w_start_str}-{w_end_str}-{coords_part}"


class MicroBatch:
    """Represents a buffered 5-minute micro-batch window."""

    def __init__(self, window_start: datetime, window_end: datetime):
        self.window_start = window_start
        self.window_end = window_end
        self.events: List[TransactionEvent] = []
        self.valid_records: List[Dict[str, Any]] = []
        self.quarantined_records: List[Dict[str, Any]] = []
        self.source_offsets: Dict[str, int] = {}  # partition -> max_offset
        self.tracked_offsets: List[Tuple[int, int]] = []
        self.duplicate_count: int = 0
        self.dlq_count: int = 0
        self.failed_count: int = 0
        self._custom_batch_id: Optional[str] = None

    @property
    def batch_id(self) -> str:
        if self._custom_batch_id:
            return self._custom_batch_id
        partition_ranges = {}
        for p, off in self.tracked_offsets:
            p_str = str(p)
            if p_str not in partition_ranges:
                partition_ranges[p_str] = (off, off)
            else:
                cur_min, cur_max = partition_ranges[p_str]
                partition_ranges[p_str] = (min(cur_min, off), max(cur_max, off))

        return make_deterministic_batch_id(
            window_start=self.window_start,
            window_end=self.window_end,
            source_offsets=self.source_offsets,
            events=self.events,
            partition_ranges=partition_ranges if partition_ranges else None,
        )

    @batch_id.setter
    def batch_id(self, val: str) -> None:
        self._custom_batch_id = val

    @property
    def total_count(self) -> int:
        return (
            len(self.valid_records)
            + len(self.quarantined_records)
            + self.duplicate_count
            + self.dlq_count
            + self.failed_count
        )


class MicroBatcher:
    """Buffers stream events into 5-minute micro-batches and coordinates monitoring runs."""

    def __init__(
        self,
        batch_interval_seconds: int = 300,
        max_batch_size: int = 1000,
        dedup_window_size: int = 50000,
        monitoring_service: Optional[MonitoringService] = None,
        model_service: Optional[ModelService] = None,
        metrics: Optional[StreamingMetrics] = None,
    ):
        self.batch_interval_seconds = batch_interval_seconds
        self.max_batch_size = max_batch_size
        self.dedup_window_size = dedup_window_size
        self.monitoring_service = monitoring_service or MonitoringService()
        self.model_service = model_service or ModelService.get_instance()
        self.metrics = metrics or get_streaming_metrics()

        # In-memory sliding set of processed event IDs for deduplication
        self._seen_event_ids: OrderedDict[str, float] = OrderedDict()
        self._current_batch: Optional[MicroBatch] = None
        self._flushed_windows: Set[Tuple[datetime, datetime]] = set()

    def _is_duplicate(self, event_id: str) -> bool:
        """Check and update deduplication cache."""
        if event_id in self._seen_event_ids:
            return True
        self._seen_event_ids[event_id] = time.time()
        # Trim cache if exceeding threshold
        if len(self._seen_event_ids) > self.dedup_window_size:
            self._seen_event_ids.popitem(last=False)
        return False

    def add_event(
        self,
        event: TransactionEvent,
        partition: int = 0,
        offset: int = 0,
    ) -> Optional[Tuple[MicroBatchMetadata, Optional[MonitoringRunResponse]]]:
        """Ingest event into appropriate micro-batch.
        If window elapses or max_batch_size is reached, triggers batch flush.
        Returns (metadata, response) if flush occurred, or None if buffered.
        """
        w_start, w_end = align_to_window(event.timestamp, self.batch_interval_seconds)

        flush_result = None

        # 1. Late arrival check: has this window already flushed?
        if (w_start, w_end) in self._flushed_windows:
            logger.warning(
                f"[Batcher] Late event '{event.event_id}' (timestamp {event.timestamp.isoformat()}) "
                f"arrived after its window [{w_start.isoformat()} - {w_end.isoformat()}) already flushed. "
                "Quarantining late event to preserve immutable historical batch."
            )
            # Ensure an active batch exists to track offset and quarantine record
            if self._current_batch is None:
                now = datetime.now(timezone.utc)
                cur_w_start, cur_w_end = align_to_window(now, self.batch_interval_seconds)
                self._current_batch = MicroBatch(window_start=cur_w_start, window_end=cur_w_end)

            p_key = str(partition)
            cur_max = self._current_batch.source_offsets.get(p_key, -1)
            if offset > cur_max:
                self._current_batch.source_offsets[p_key] = offset
            self._current_batch.tracked_offsets.append((partition, offset))

            if self._is_duplicate(event.event_id):
                self._current_batch.duplicate_count += 1
                self.metrics.record_event_duplicated()
                return None

            # Quarantine late event in current batch with explicit finding
            late_features = dict(event.features)
            late_features["__late_event_error"] = (
                f"Late event: window [{w_start.isoformat()} - {w_end.isoformat()}) already flushed"
            )
            self._current_batch.quarantined_records.append(late_features)
            self._current_batch.events.append(event)
            self.metrics.record_event_quarantined()

            if self._current_batch.total_count >= self.max_batch_size:
                flush_result = self.flush_current_batch()
            return flush_result

        # 2. Check if window transition occurred (forward window progression)
        if self._current_batch is not None:
            if (
                event.timestamp < self._current_batch.window_start
                or event.timestamp >= self._current_batch.window_end
                or self._current_batch.total_count >= self.max_batch_size
            ):
                flush_result = self.flush_current_batch()

        # 3. Initialize new batch if none exists
        if self._current_batch is None:
            self._current_batch = MicroBatch(window_start=w_start, window_end=w_end)

        # Update partition offset tracking
        p_key = str(partition)
        cur_max = self._current_batch.source_offsets.get(p_key, -1)
        if offset > cur_max:
            self._current_batch.source_offsets[p_key] = offset
        self._current_batch.tracked_offsets.append((partition, offset))

        # Deduplication check
        if self._is_duplicate(event.event_id):
            self._current_batch.duplicate_count += 1
            self.metrics.record_event_duplicated()
            logger.info(f"[Batcher] Duplicate event ignored: event_id={event.event_id}")
            return flush_result

        # Feature validation using ModelService
        is_valid, error_reason = self.model_service.validate_features(event.features)
        if is_valid:
            self._current_batch.valid_records.append(event.features)
            self.metrics.record_event_validated()
        else:
            self._current_batch.quarantined_records.append(event.features)
            self.metrics.record_event_quarantined()
            logger.debug(
                f"[Batcher] Quarantined record event_id={event.event_id}: reason={error_reason}"
            )

        self._current_batch.events.append(event)

        # Flush immediately if max_batch_size reached
        if self._current_batch.total_count >= self.max_batch_size:
            flush_result = self.flush_current_batch()

        return flush_result

    def record_dlq_event(self, partition: int = 0, offset: int = 0) -> None:
        """Record DLQ handoff in current batch tracking."""
        if self._current_batch is None:
            now = datetime.now(timezone.utc)
            w_start, w_end = align_to_window(now, self.batch_interval_seconds)
            self._current_batch = MicroBatch(window_start=w_start, window_end=w_end)

        p_key = str(partition)
        cur_max = self._current_batch.source_offsets.get(p_key, -1)
        if offset > cur_max:
            self._current_batch.source_offsets[p_key] = offset
        self._current_batch.tracked_offsets.append((partition, offset))
        self._current_batch.dlq_count += 1

    def flush_current_batch(
        self, persist: bool = True
    ) -> Tuple[MicroBatchMetadata, Optional[MonitoringRunResponse]]:
        """Flush the active micro-batch, run monitoring orchestration, and reset state."""
        if self._current_batch is None:
            now = datetime.now(timezone.utc)
            w_start, w_end = align_to_window(now, self.batch_interval_seconds)
            meta = MicroBatchMetadata(
                window_start=w_start,
                window_end=w_end,
                status="empty",
            )
            return meta, None

        batch = self._current_batch
        self._current_batch = None  # Reset active batch

        start_time_proc = time.perf_counter()

        received_count = (
            len(batch.valid_records)
            + len(batch.quarantined_records)
            + batch.duplicate_count
            + batch.dlq_count
            + batch.failed_count
        )
        valid_count = len(batch.valid_records)
        quarantined_count = len(batch.quarantined_records)

        # Handle empty batch
        if (valid_count + quarantined_count) == 0:
            duration_ms = (time.perf_counter() - start_time_proc) * 1000
            meta = MicroBatchMetadata(
                batch_id=batch.batch_id,
                window_start=batch.window_start,
                window_end=batch.window_end,
                processing_time_ms=round(duration_ms, 2),
                source_offsets=batch.source_offsets,
                received_count=received_count,
                valid_count=0,
                quarantined_count=0,
                duplicate_count=batch.duplicate_count,
                dlq_count=batch.dlq_count,
                failed_count=batch.failed_count,
                status="empty",
            )
            return meta, None

        # Assemble full dataset for monitoring run
        all_records = batch.valid_records + batch.quarantined_records

        # Execute monitoring run via MonitoringService
        run_request = MonitoringRunRequest(
            model_name=self.model_service.model_name,
            model_version=self.model_service.model_version,
            run_type="streaming_microbatch",
            current_data=all_records,
            ground_truth_labels=None,  # Delayed labels
            run_id=f"run-{batch.batch_id}",
        )

        try:
            run_response = self.monitoring_service.execute_monitoring_run(
                run_request, persist=persist
            )
        except Exception as exc:
            logger.error(
                f"[Batcher] Monitoring run execution or persistence failed for batch {batch.batch_id}: {exc}",
                exc_info=True,
            )
            # DO NOT claim success; propagate exception so consumer does NOT commit offsets
            raise RuntimeError(
                f"Micro-batch monitoring persistence failure for batch {batch.batch_id}: {exc}"
            ) from exc

        duration_ms = (time.perf_counter() - start_time_proc) * 1000

        status = "completed"
        if valid_count == 0 and quarantined_count > 0:
            status = "all_quarantined"

        meta = MicroBatchMetadata(
            batch_id=batch.batch_id,
            window_start=batch.window_start,
            window_end=batch.window_end,
            processing_time_ms=round(duration_ms, 2),
            source_offsets=batch.source_offsets,
            tracked_offsets=list(batch.tracked_offsets),
            received_count=received_count,
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            duplicate_count=batch.duplicate_count,
            dlq_count=batch.dlq_count,
            failed_count=batch.failed_count,
            monitoring_run_id=run_response.run_id,
            status=status,
        )

        logger.info(
            f"[Batcher] Completed micro-batch {batch.batch_id}: "
            f"received={received_count} valid={valid_count} quar={quarantined_count} "
            f"dup={batch.duplicate_count} dlq={batch.dlq_count} "
            f"monitoring_run_id={run_response.run_id} status={status}"
        )

        self._flushed_windows.add((batch.window_start, batch.window_end))
        return meta, run_response

    @property
    def current_batch_id(self) -> Optional[str]:
        """Return active batch ID if one is buffering."""
        return self._current_batch.batch_id if self._current_batch else None

    def has_pending_records(self, partition: int) -> bool:
        """Check if active batch has unpersisted records on partition."""
        if self._current_batch is None:
            return False
        p_str = str(partition)
        return (p_str in self._current_batch.source_offsets) and (
            len(self._current_batch.valid_records) + len(self._current_batch.quarantined_records) > 0
        )
