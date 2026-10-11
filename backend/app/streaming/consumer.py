"""ModelPulse Streaming Consumer (Phase 5).
Coordinates:
- Message consumption and envelope validation.
- Dead Letter Queue routing for malformed/unsupported messages.
- Five-minute micro-batch accumulation.
- Bounded retries with exponential backoff for transient failures.
- Strict manual offset commit policy (offsets committed only after durable persistence or DLQ handoff).
- Graceful shutdown and rebalance safety.
"""
import asyncio
from datetime import datetime, timezone
from enum import Enum
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

from backend.app.schemas.events import (
    TransactionEvent,
    DLQEvent,
    MicroBatchMetadata,
)
from backend.app.schemas.monitoring import MonitoringRunResponse
from backend.app.streaming.bus import BusMessage, MessageBus, get_message_bus
from backend.app.streaming.dlq import DeadLetterQueueService
from backend.app.streaming.metrics import StreamingMetrics, get_streaming_metrics
from backend.app.streaming.micro_batcher import MicroBatcher

logger = logging.getLogger(__name__)


class RecordStatus(Enum):
    PENDING = "pending"
    RESOLVED = "resolved"
    FAILED = "failed"


class PartitionOffsetTracker:
    """Tracks per-partition offset progression ensuring strict contiguity and low-watermark safety.
    Guarantees:
    - Offset commits are tracked independently for every topic partition.
    - A partition's committed offset never advances past an earlier record whose required processing,
      persistence, or DLQ handoff has not succeeded.
    - Non-zero starting offsets, broker offset gaps (e.g. compacted or control records), and revisited
      offsets on consumer replay/rewind are handled safely without losing the committed cursor.
    - Partition revocation and reassignment state resets are supported cleanly.
    - Kafka next-offset commit semantics: tracks last-processed offset, commits next expected fetch.
    """

    def __init__(self):
        # partition -> {offset: status}
        self._offsets: Dict[int, Dict[int, RecordStatus]] = {}
        # partition -> last committed processed offset
        self._last_committed: Dict[int, int] = {}
        # partition -> {offset: batch_id}
        self._batch_mapping: Dict[int, Dict[int, str]] = {}

    def track_record(
        self,
        partition: int,
        offset: int,
        status: RecordStatus = RecordStatus.PENDING,
        batch_id: Optional[str] = None,
    ) -> None:
        if partition not in self._offsets:
            self._offsets[partition] = {}
            self._batch_mapping[partition] = {}
        # If partition uninitialized, or if an earlier offset is revisited (e.g. stream rewind/replay)
        if partition not in self._last_committed or offset <= self._last_committed[partition]:
            self._last_committed[partition] = offset - 1
            # Clean up any stale records strictly ahead of the rewind offset
            stale_offsets = [o for o in self._offsets[partition].keys() if o >= offset]
            for o in stale_offsets:
                del self._offsets[partition][o]
                self._batch_mapping[partition].pop(o, None)

        self._offsets[partition][offset] = status
        if batch_id:
            self._batch_mapping[partition][offset] = batch_id

    def mark_resolved(self, partition: int, offset: int) -> None:
        if partition not in self._offsets:
            self._offsets[partition] = {}
        if partition not in self._last_committed or offset <= self._last_committed[partition]:
            self._last_committed[partition] = offset - 1
        self._offsets[partition][offset] = RecordStatus.RESOLVED

    def mark_failed(self, partition: int, offset: int) -> None:
        if partition in self._offsets and offset in self._offsets[partition]:
            self._offsets[partition][offset] = RecordStatus.FAILED

    def mark_batch_resolved(self, batch_id: str) -> None:
        for p, mapping in self._batch_mapping.items():
            for off, b_id in mapping.items():
                if b_id == batch_id:
                    self._offsets[p][off] = RecordStatus.RESOLVED

    def mark_batch_failed(self, batch_id: str) -> None:
        for p, mapping in self._batch_mapping.items():
            for off, b_id in mapping.items():
                if b_id == batch_id:
                    self._offsets[p][off] = RecordStatus.FAILED

    def has_pending(self, partition: int, up_to_offset: Optional[int] = None) -> bool:
        if partition not in self._offsets:
            return False
        for off, status in self._offsets[partition].items():
            if up_to_offset is not None and off > up_to_offset:
                continue
            if status == RecordStatus.PENDING:
                return True
        return False

    def get_committable_offset(self, partition: int) -> Optional[int]:
        """Compute highest contiguous resolved offset that can safely be committed to Kafka.
        Safely handles non-zero starting offsets, broker offset gaps (e.g. compacted or
        control batches), and never advances past any earlier PENDING or FAILED record.
        """
        if partition not in self._offsets or not self._offsets[partition]:
            return None

        last_comm = self._last_committed.get(partition, -1)
        safe_to_commit = None

        # Inspect tracked offsets in ascending order
        for off in sorted(self._offsets[partition].keys()):
            if off <= last_comm:
                continue
            status = self._offsets[partition][off]
            if status == RecordStatus.RESOLVED:
                safe_to_commit = off
            else:
                # Met a PENDING or FAILED record - STOP immediately!
                # Committing any offset >= this record would skip it on restart.
                break

        return safe_to_commit

    def record_committed(self, partition: int, offset: int) -> None:
        """Record that Kafka has committed up to `offset` for `partition` and prune older history."""
        self._last_committed[partition] = offset
        if partition in self._offsets:
            to_remove = [o for o in self._offsets[partition].keys() if o <= offset]
            for o in to_remove:
                del self._offsets[partition][o]
                self._batch_mapping[partition].pop(o, None)

    def reset_partition(self, partition: int, initial_committed: Optional[int] = None) -> None:
        """Reset tracking state for a partition (e.g. upon partition revocation or reassignment)."""
        self._offsets.pop(partition, None)
        self._batch_mapping.pop(partition, None)
        if initial_committed is not None:
            self._last_committed[partition] = initial_committed
        else:
            self._last_committed.pop(partition, None)

    def reset_all(self) -> None:
        """Reset all tracked partitions (e.g. full consumer group rebalance)."""
        self._offsets.clear()
        self._last_committed.clear()
        self._batch_mapping.clear()


class StreamingConsumer:
    """Async consumer processing transaction events into micro-batches with DLQ protection."""

    def __init__(
        self,
        topic: str = "events",
        group_id: str = "modelpulse-consumer-group",
        dlq_topic: str = "events.dlq",
        results_topic: Optional[str] = "monitoring.results",
        message_bus: Optional[MessageBus] = None,
        dlq_service: Optional[DeadLetterQueueService] = None,
        micro_batcher: Optional[MicroBatcher] = None,
        max_retries: int = 3,
        retry_backoff_ms: int = 500,
        metrics: Optional[StreamingMetrics] = None,
    ):
        self.topic = topic
        self.group_id = group_id
        self.dlq_topic = dlq_topic
        self.results_topic = results_topic
        self.message_bus = message_bus or get_message_bus()
        self.dlq_service = dlq_service or DeadLetterQueueService(
            dlq_topic=dlq_topic, message_bus=self.message_bus
        )
        self.metrics = metrics or get_streaming_metrics()
        self.micro_batcher = micro_batcher or MicroBatcher(metrics=self.metrics)
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.offset_tracker = PartitionOffsetTracker()

        self._running: bool = False
        self._completed_batches: List[Tuple[MicroBatchMetadata, Optional[MonitoringRunResponse]]] = []

    async def _handle_dlq_success(self, partition: int, offset: int) -> None:
        """Record DLQ handoff and commit offset only if contiguous with prior processed records."""
        self.offset_tracker.track_record(partition, offset, RecordStatus.RESOLVED)
        self.micro_batcher.record_dlq_event(partition, offset)
        safe_offset = self.offset_tracker.get_committable_offset(partition)
        if safe_offset is not None:
            await self.message_bus.commit_offset(self.topic, self.group_id, partition, safe_offset)
            self.offset_tracker.record_committed(partition, safe_offset)
            logger.debug(
                f"[Consumer] Committed offset partition={partition} offset={safe_offset} after DLQ handoff."
            )

    async def process_single_message(self, msg: BusMessage) -> None:
        """Process a single Kafka message:
        1. Validates JSON and schema version.
        2. Routes unrecoverable messages immediately to DLQ; commits offset safely.
        3. Buffers valid envelopes into MicroBatcher.
        4. If a micro-batch flushes and persists, commits offsets for the completed batch.
        """
        self.metrics.record_event_consumed()
        raw_val = msg.value
        correlation_id = msg.headers.get("correlation_id", str(uuid.uuid4()))

        # 1. Parse JSON payload
        try:
            data = json.loads(raw_val)
        except Exception as exc:
            logger.warning(f"[Consumer] Malformed JSON payload at offset {msg.offset}: {exc}")
            self.metrics.record_event_dlq()
            try:
                await self.dlq_service.route_to_dlq(
                    original_payload=raw_val,
                    failure_category="malformed_json",
                    diagnostic_message=f"JSON decode error: {exc}",
                    event_id=None,
                    correlation_id=correlation_id,
                )
                await self._handle_dlq_success(msg.partition, msg.offset)
            except Exception as dlq_err:
                self.offset_tracker.track_record(msg.partition, msg.offset, RecordStatus.FAILED)
                self.metrics.record_persistence_error()
                logger.error(f"[Consumer] DLQ publication failed: {dlq_err}")
                raise
            return

        if not isinstance(data, dict):
            logger.warning(f"[Consumer] Non-dictionary JSON payload at offset {msg.offset}")
            self.metrics.record_event_dlq()
            try:
                await self.dlq_service.route_to_dlq(
                    original_payload=raw_val,
                    failure_category="malformed_json",
                    diagnostic_message="Payload must be a JSON object (key-value dictionary)",
                    event_id=None,
                    correlation_id=correlation_id,
                )
                await self._handle_dlq_success(msg.partition, msg.offset)
            except Exception as dlq_err:
                self.offset_tracker.track_record(msg.partition, msg.offset, RecordStatus.FAILED)
                self.metrics.record_persistence_error()
                logger.error(f"[Consumer] DLQ publication failed: {dlq_err}")
                raise
            return

        # 2. Validate envelope and schema version
        event_id = data.get("event_id")
        schema_version = data.get("schema_version")

        if schema_version != "1.0.0":
            logger.warning(
                f"[Consumer] Unsupported schema version '{schema_version}' for event {event_id}"
            )
            self.metrics.record_event_dlq()
            try:
                await self.dlq_service.route_to_dlq(
                    original_payload=raw_val,
                    failure_category="unsupported_schema_version",
                    diagnostic_message=f"Unsupported schema version '{schema_version}'. Supported: ['1.0.0']",
                    event_id=event_id,
                    correlation_id=correlation_id,
                )
                await self._handle_dlq_success(msg.partition, msg.offset)
            except Exception as dlq_err:
                self.offset_tracker.track_record(msg.partition, msg.offset, RecordStatus.FAILED)
                self.metrics.record_persistence_error()
                logger.error(f"[Consumer] DLQ publication failed: {dlq_err}")
                raise
            return

        # 3. Construct and validate TransactionEvent envelope
        try:
            event = TransactionEvent.model_validate(data)
        except Exception as exc:
            logger.warning(f"[Consumer] Invalid TransactionEvent envelope for event {event_id}: {exc}")
            self.metrics.record_event_dlq()
            try:
                await self.dlq_service.route_to_dlq(
                    original_payload=raw_val,
                    failure_category="validation_error",
                    diagnostic_message=f"Pydantic envelope validation error: {exc}",
                    event_id=event_id,
                    correlation_id=correlation_id,
                )
                await self._handle_dlq_success(msg.partition, msg.offset)
            except Exception as dlq_err:
                self.offset_tracker.track_record(msg.partition, msg.offset, RecordStatus.FAILED)
                self.metrics.record_persistence_error()
                logger.error(f"[Consumer] DLQ publication failed: {dlq_err}")
                raise
            return

        # 4. Ingest into micro-batcher with bounded retries for transient processing errors
        attempt = 0
        assigned_batch_id = self.micro_batcher.current_batch_id or "pending"
        self.offset_tracker.track_record(
            msg.partition, msg.offset, RecordStatus.PENDING, batch_id=assigned_batch_id
        )

        while attempt <= self.max_retries:
            attempt += 1
            try:
                flush_result = self.micro_batcher.add_event(
                    event=event,
                    partition=msg.partition,
                    offset=msg.offset,
                )

                # If micro-batch flushed, finalize batch and commit offsets
                if flush_result is not None:
                    await self._handle_flushed_batch(flush_result)

                # Return on successful processing
                return

            except Exception as exc:
                err_msg = str(exc).lower()
                if (
                    "persistence failure" in err_msg
                    or "database outage" in err_msg
                    or "persistence transaction failed" in err_msg
                    or "disk i/o error" in err_msg
                    or "alert outbox staging failure" in err_msg
                ):
                    self.metrics.record_batch_failed()
                    self.metrics.record_persistence_error()
                    logger.error(
                        f"[Consumer] Batch persistence failure during add_event at offset {msg.offset}: {exc}"
                    )
                    raise

                self.metrics.record_event_retried()
                logger.warning(
                    f"[Consumer] Transient error processing event {event_id} (attempt {attempt}/{self.max_retries}): {exc}"
                )
                if attempt <= self.max_retries:
                    backoff_secs = (self.retry_backoff_ms * (2 ** (attempt - 1))) / 1000.0
                    await asyncio.sleep(backoff_secs)
                else:
                    # Bounded retries exhausted: route to DLQ
                    self.metrics.record_retry_exhausted()
                    self.metrics.record_event_dlq()
                    logger.error(
                        f"[Consumer] Retry exhaustion for event {event_id}. Routing to DLQ: {exc}"
                    )
                    try:
                        await self.dlq_service.route_to_dlq(
                            original_payload=raw_val,
                            failure_category="retry_exhausted",
                            diagnostic_message=f"Retries exhausted ({self.max_retries} attempts): {exc}",
                            event_id=event_id,
                            attempt_count=attempt,
                            correlation_id=correlation_id,
                        )
                        await self._handle_dlq_success(msg.partition, msg.offset)
                    except Exception as dlq_err:
                        self.offset_tracker.track_record(msg.partition, msg.offset, RecordStatus.FAILED)
                        self.metrics.record_persistence_error()
                        logger.error(f"[Consumer] DLQ publication failed after retry exhaustion: {dlq_err}")
                        raise
                    return

    async def _handle_flushed_batch(
        self, flush_result: Tuple[MicroBatchMetadata, Optional[MonitoringRunResponse]]
    ) -> None:
        """Handle completed and persisted micro-batch:
        1. Mark batch as resolved in offset tracker.
        2. Commit safe contiguous offsets for all partitions represented in the batch.
        3. Optionally publish monitoring summary to results topic.
        4. Record metrics and audit history.
        """
        meta, run_resp = flush_result
        self._completed_batches.append(flush_result)
        # Stage alert outbox records into persistent repository BEFORE Kafka offset commits
        if run_resp is not None:
            try:
                from backend.app.alerting.service import get_alert_service
                get_alert_service().stage_outbox(run_resp)
            except Exception as stg_exc:
                logger.error(
                    f"[Consumer] Failed staging alert outbox for batch {meta.batch_id}: {stg_exc}. "
                    "Aborting batch finalization and offset commit to prevent alert loss."
                )
                raise RuntimeError(
                    f"Alert outbox staging failure for batch {meta.batch_id}: {stg_exc}"
                ) from stg_exc

        # Mark all records belonging to this batch as resolved
        for p, off in meta.tracked_offsets:
            self.offset_tracker.mark_resolved(p, off)
        for p_str, highest_off in meta.source_offsets.items():
            self.offset_tracker.mark_resolved(int(p_str), highest_off)

        # Commit highest contiguous safe offsets for each partition
        for p_str in meta.source_offsets.keys():
            p_int = int(p_str)
            safe_off = self.offset_tracker.get_committable_offset(p_int)
            if safe_off is not None:
                await self.message_bus.commit_offset(
                    topic=self.topic,
                    group_id=self.group_id,
                    partition=p_int,
                    offset=safe_off,
                )
                self.offset_tracker.record_committed(p_int, safe_off)
                logger.debug(
                    f"[Consumer] Committed safe offset partition={p_int} offset={safe_off} "
                    f"for batch={meta.batch_id}"
                )

        # Optionally publish summary to results topic if configured and monitoring ran
        if self.results_topic and run_resp is not None:
            try:
                summary_payload = json.dumps({
                    "batch_id": meta.batch_id,
                    "run_id": run_resp.run_id,
                    "model_name": run_resp.model_name,
                    "model_version": run_resp.model_version,
                    "severity_score": run_resp.severity.score,
                    "severity_band": run_resp.severity.band,
                    "sample_count": run_resp.sample_count,
                    "valid_sample_count": run_resp.valid_sample_count,
                    "quarantined_sample_count": run_resp.quarantined_sample_count,
                    "dataset_drift_detected": run_resp.dataset_drift_detected,
                    "created_at": run_resp.created_at.isoformat(),
                })
                await self.message_bus.publish(
                    topic=self.results_topic,
                    value=summary_payload,
                    key=run_resp.run_id,
                )
            except Exception as exc:
                logger.warning(f"[Consumer] Failed to publish results summary: {exc}")

        # Dispatch alerts if monitoring ran
        if run_resp is not None:
            try:
                from backend.app.alerting.service import get_alert_service
                await get_alert_service().evaluate_and_dispatch(run_resp)
            except Exception as alt_exc:
                logger.warning(f"[Consumer] Failed to evaluate alerts for batch {meta.batch_id}: {alt_exc}")


    async def process_available_messages(self, max_messages: int = 1000) -> int:
        """Fetch and process available uncommitted messages in bulk (useful for testing or batch cycles)."""
        messages = await self.message_bus.consume(
            topic=self.topic,
            group_id=self.group_id,
            max_records=max_messages,
            timeout_ms=500,
        )
        for msg in messages:
            await self.process_single_message(msg)
        return len(messages)

    async def flush_and_commit(
        self, persist: bool = True
    ) -> Optional[Tuple[MicroBatchMetadata, Optional[MonitoringRunResponse]]]:
        """Manually flush the pending micro-batch and commit offsets."""
        try:
            flush_result = self.micro_batcher.flush_current_batch(persist=persist)
            if flush_result is not None:
                await self._handle_flushed_batch(flush_result)
            return flush_result
        except Exception as exc:
            self.metrics.record_batch_failed()
            self.metrics.record_persistence_error()
            if self.micro_batcher.current_batch_id:
                self.offset_tracker.mark_batch_failed(self.micro_batcher.current_batch_id)
            logger.error(f"[Consumer] Flush and commit failed: {exc}")
            raise

    def get_completed_batches(
        self,
    ) -> List[Tuple[MicroBatchMetadata, Optional[MonitoringRunResponse]]]:
        """Return audit history of completed micro-batches."""
        return list(self._completed_batches)

    async def run_loop(self, poll_interval_ms: int = 500) -> None:
        """Continuous consumption loop with graceful shutdown support."""
        self._running = True
        logger.info(f"[Consumer] Starting run loop on topic {self.topic} (group: {self.group_id})")

        try:
            while self._running:
                n = await self.process_available_messages(max_messages=100)
                if n == 0:
                    await asyncio.sleep(poll_interval_ms / 1000.0)
        except asyncio.CancelledError:
            logger.info("[Consumer] Cancellation requested.")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully shut down consumer, flushing any buffered work."""
        self._running = False
        logger.info("[Consumer] Graceful shutdown initiated: flushing pending batch...")
        try:
            await self.flush_and_commit(persist=True)
        except Exception as exc:
            logger.warning(f"[Consumer] Error during shutdown batch flush: {exc}")
        logger.info("[Consumer] Graceful shutdown complete.")

    def on_partitions_revoked(self, partitions: List[int]) -> None:
        """Called when partitions are revoked during a Kafka consumer rebalance.
        Cleans up partition offset tracking state to prevent cursor corruption.
        """
        logger.info(f"[Consumer] Partitions revoked: {partitions}")
        for p in partitions:
            self.offset_tracker.reset_partition(p)

    def on_partitions_assigned(self, partitions: List[int]) -> None:
        """Called when partitions are assigned during a Kafka consumer rebalance.
        Initializes partition offset tracking state cleanly for newly assigned partitions.
        """
        logger.info(f"[Consumer] Partitions assigned: {partitions}")
        for p in partitions:
            self.offset_tracker.reset_partition(p)

