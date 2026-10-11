"""ModelPulse Phase 5.2 Verification & Correctness Tests.
Audits:
1. Offset tracker correctness:
   - Non-zero starting offsets.
   - Broker offset gaps (compacted logs, transactional control batches).
   - Revisiting previously committed offsets (consumer seek, rewind, redelivery).
   - Batch A failure followed by Batch B success holding watermark.
   - Partition revocation and reassignment state resets.
2. Deterministic batch identity and idempotency:
   - Canonical batch fingerprint: identical replay, reordered replay, altered payload, partial replay, window boundary crossing.
   - Real SQLite repository uniqueness and idempotency validation.
3. Event-time policy:
   - Boundary timestamps (window start, epsilon before end, exact boundary).
   - Out-of-order events within the same window.
   - Late events arriving after window flush quarantined without mutating historical batches.
"""
from datetime import datetime, timezone, timedelta
import json
import pytest

from backend.app.db.repository import SQLiteMonitoringRepository
from backend.app.schemas.events import TransactionEvent
from backend.app.schemas.monitoring import MonitoringRunRequest, MonitoringRunResponse
from backend.app.services.model_service import ModelService
from backend.app.services.monitoring_service import MonitoringService
from backend.app.streaming.bus import InMemoryMessageBus
from backend.app.streaming.consumer import (
    PartitionOffsetTracker,
    RecordStatus,
    StreamingConsumer,
)
from backend.app.streaming.micro_batcher import (
    MicroBatcher,
    align_to_window,
    make_deterministic_batch_id,
)
from backend.app.streaming.producer import TransactionProducer


@pytest.fixture
def valid_features():
    return {
        "step": 1,
        "type": "PAYMENT",
        "amount": 9839.64,
        "oldbalanceOrg": 170136.0,
        "newbalanceOrig": 160296.36,
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
    }


# ==============================================================================
# 1. OFFSET TRACKER CORRECTNESS & EDGE CASES
# ==============================================================================

def test_offset_tracker_non_zero_start_and_broker_gaps():
    """Verify offset tracker handles non-zero initial offsets and broker log gaps
    (e.g. from log compaction or transactional markers) without stalling commits.
    """
    tracker = PartitionOffsetTracker()

    # Stream begins at non-zero offset 500 with gaps: 500, 502, 505
    tracker.track_record(partition=0, offset=500, status=RecordStatus.PENDING)
    tracker.track_record(partition=0, offset=502, status=RecordStatus.PENDING)
    tracker.track_record(partition=0, offset=505, status=RecordStatus.PENDING)

    assert tracker.get_committable_offset(0) is None

    # Resolve 500: safe offset is 500
    tracker.mark_resolved(0, 500)
    assert tracker.get_committable_offset(0) == 500

    # Resolve 502 across the gap (501 doesn't exist on broker): safe offset advances to 502
    tracker.mark_resolved(0, 502)
    assert tracker.get_committable_offset(0) == 502

    # Commit 502 and prune up to 502
    tracker.record_committed(0, 502)

    # 505 is still PENDING -> no committable offset
    assert tracker.get_committable_offset(0) is None

    # Resolve 505 -> safe offset is 505
    tracker.mark_resolved(0, 505)
    assert tracker.get_committable_offset(0) == 505
    tracker.record_committed(0, 505)

    # Now empty
    assert tracker.get_committable_offset(0) is None


def test_offset_tracker_revisiting_committed_offsets_on_seek_or_replay():
    """Verify offset tracker safely handles stream rewind / seek backwards to an earlier offset."""
    tracker = PartitionOffsetTracker()

    # Commit up to offset 100
    tracker.track_record(partition=0, offset=100, status=RecordStatus.RESOLVED)
    assert tracker.get_committable_offset(0) == 100
    tracker.record_committed(0, 100)

    # Stream is rewinded/seeked back to offset 40
    tracker.track_record(partition=0, offset=40, status=RecordStatus.PENDING)
    tracker.track_record(partition=0, offset=41, status=RecordStatus.PENDING)

    # Cannot commit yet
    assert tracker.get_committable_offset(0) is None

    # Resolve 40 -> safe offset is 40
    tracker.mark_resolved(0, 40)
    assert tracker.get_committable_offset(0) == 40
    tracker.record_committed(0, 40)

    # Resolve 41 -> safe offset is 41
    tracker.mark_resolved(0, 41)
    assert tracker.get_committable_offset(0) == 41


def test_offset_tracker_partition_revocation_and_reassignment():
    """Verify partition state reset when consumer group rebalances."""
    tracker = PartitionOffsetTracker()

    tracker.track_record(partition=0, offset=10, status=RecordStatus.RESOLVED)
    tracker.track_record(partition=1, offset=20, status=RecordStatus.PENDING)

    # Partition 0 is revoked
    tracker.reset_partition(partition=0)
    assert tracker.get_committable_offset(0) is None

    # Partition 1 remains intact
    assert tracker.has_pending(partition=1) is True

    # Reassigned partition 0 with fresh offset 50
    tracker.track_record(partition=0, offset=50, status=RecordStatus.RESOLVED)
    assert tracker.get_committable_offset(0) == 50


def test_batch_a_failure_followed_by_batch_b_success_holds_watermark():
    """Verify that if batch A fails persistence and batch B succeeds,
    commits NEVER leapfrog batch A's unresolved records.
    """
    tracker = PartitionOffsetTracker()

    # Batch A: offsets 10, 11
    tracker.track_record(0, 10, status=RecordStatus.PENDING, batch_id="batch-A")
    tracker.track_record(0, 11, status=RecordStatus.PENDING, batch_id="batch-A")

    # Batch B: offsets 12, 13
    tracker.track_record(0, 12, status=RecordStatus.PENDING, batch_id="batch-B")
    tracker.track_record(0, 13, status=RecordStatus.PENDING, batch_id="batch-B")

    # Batch A fails
    tracker.mark_batch_failed("batch-A")
    # Batch B succeeds
    tracker.mark_batch_resolved("batch-B")

    # Committable offset must be None (held back by failed offsets 10, 11)
    assert tracker.get_committable_offset(0) is None


# ==============================================================================
# 2. DETERMINISTIC BATCH IDENTITY & CANONICAL FINGERPRINT
# ==============================================================================

def test_canonical_batch_fingerprint_invariants(valid_features):
    """Verify canonical batch fingerprint properties:
    - Identical replay produces identical ID.
    - Reordered replay produces identical ID.
    - Altered payload under same event ID produces different ID.
    - Partial batch produces different ID.
    - Window boundary change produces different ID.
    """
    w_start = datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)
    w_end = datetime(2026, 10, 9, 14, 5, 0, tzinfo=timezone.utc)
    offsets = {"0": 10}

    e1 = TransactionEvent(
        event_id="e-1",
        timestamp=w_start + timedelta(seconds=10),
        features=valid_features,
    )
    e2 = TransactionEvent(
        event_id="e-2",
        timestamp=w_start + timedelta(seconds=20),
        features={**valid_features, "amount": 500.0},
    )

    # Baseline ID
    id_base = make_deterministic_batch_id(w_start, w_end, offsets, events=[e1, e2])

    # 1. Identical replay
    id_identical = make_deterministic_batch_id(w_start, w_end, offsets, events=[e1, e2])
    assert id_identical == id_base

    # 2. Reordered replay: [e2, e1] instead of [e1, e2]
    id_reordered = make_deterministic_batch_id(w_start, w_end, offsets, events=[e2, e1])
    assert id_reordered == id_base, "Canonical fingerprint must be order-invariant for identical event sets"

    # 3. Altered payload under the same event ID (amount changed on e1)
    e1_altered = TransactionEvent(
        event_id="e-1",
        timestamp=w_start + timedelta(seconds=10),
        features={**valid_features, "amount": 999999.0},
    )
    id_altered = make_deterministic_batch_id(w_start, w_end, offsets, events=[e1_altered, e2])
    assert id_altered != id_base, "Altered event payload must produce a distinct batch fingerprint"

    # 4. Partial batch: only e1
    id_partial = make_deterministic_batch_id(w_start, w_end, offsets, events=[e1])
    assert id_partial != id_base

    # 5. Window boundary shift
    w_start_shifted = w_start + timedelta(minutes=5)
    w_end_shifted = w_end + timedelta(minutes=5)
    id_shifted = make_deterministic_batch_id(w_start_shifted, w_end_shifted, offsets, events=[e1, e2])
    assert id_shifted != id_base


def test_sqlite_repository_uniqueness_and_idempotency_audit(valid_features):
    """Verify repository-level ACID uniqueness and MonitoringService idempotency with real SQLite."""
    repo = SQLiteMonitoringRepository(db_path=":memory:")
    model_svc = ModelService.get_instance()
    mon_svc = MonitoringService(repository=repo, model_service=model_svc)

    req = MonitoringRunRequest(
        model_name=model_svc.model_name,
        model_version=model_svc.model_version,
        run_type="streaming_microbatch",
        current_data=[valid_features],
        run_id="audit-run-idempotent-100",
    )

    # 1. First execution: executes and persists into SQLite
    resp1 = mon_svc.execute_monitoring_run(req, persist=True)
    assert resp1.run_id == "audit-run-idempotent-100"
    assert repo.count_runs() == 1

    # 2. Replay with identical run_id: returns existing run idempotently without re-execution
    resp2 = mon_svc.execute_monitoring_run(req, persist=True)
    assert resp2.run_id == "audit-run-idempotent-100"
    assert repo.count_runs() == 1
    assert resp1.created_at == resp2.created_at

    # 3. Direct SQLite uniqueness assertion: attempting to force-save a conflicting run raises ValueError
    conflicting_run = MonitoringRunResponse.model_validate({
        **resp1.model_dump(),
        "sample_count": 9999,
    })
    with pytest.raises(ValueError, match="Duplicate run_id"):
        repo.save_run(conflicting_run)

    # Confirm original persisted record is strictly unmutated
    stored = repo.get_run("audit-run-idempotent-100")
    assert stored.sample_count == resp1.sample_count


# ==============================================================================
# 3. EVENT-TIME POLICY & LATE-EVENT BEHAVIOR
# ==============================================================================

def test_window_boundary_timestamps():
    """Verify precise 5-minute window boundary alignment semantics."""
    interval = 300  # 5 minutes

    # Start boundary: 14:00:00.000 -> [14:00:00, 14:05:00)
    t_start = datetime(2026, 10, 9, 14, 0, 0, 0, tzinfo=timezone.utc)
    s1, e1 = align_to_window(t_start, interval)
    assert s1 == datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)
    assert e1 == datetime(2026, 10, 9, 14, 5, 0, tzinfo=timezone.utc)

    # Epsilon before boundary: 14:04:59.999999 -> [14:00:00, 14:05:00)
    t_sub = datetime(2026, 10, 9, 14, 4, 59, 999999, tzinfo=timezone.utc)
    s2, e2 = align_to_window(t_sub, interval)
    assert s2 == s1
    assert e2 == e1

    # Exact boundary: 14:05:00.000000 -> [14:05:00, 14:10:00)
    t_next = datetime(2026, 10, 9, 14, 5, 0, 0, tzinfo=timezone.utc)
    s3, e3 = align_to_window(t_next, interval)
    assert s3 == datetime(2026, 10, 9, 14, 5, 0, tzinfo=timezone.utc)
    assert e3 == datetime(2026, 10, 9, 14, 10, 0, tzinfo=timezone.utc)


def test_out_of_order_events_within_same_window_buffered_cleanly(valid_features):
    """Verify that out-of-order events within the same 5-minute window
    are buffered into the same batch without triggering an unexpected flush.
    """
    repo = SQLiteMonitoringRepository(db_path=":memory:")
    mon_svc = MonitoringService(repository=repo)
    batcher = MicroBatcher(batch_interval_seconds=300, max_batch_size=100, monitoring_service=mon_svc)

    # Event 1: timestamp 14:03
    t1 = datetime(2026, 10, 9, 14, 3, 0, tzinfo=timezone.utc)
    e1 = TransactionEvent(event_id="ooo-1", timestamp=t1, features=valid_features)
    flush1 = batcher.add_event(e1, partition=0, offset=0)
    assert flush1 is None

    # Event 2: timestamp 14:01 (arrives AFTER Event 1, but belongs to same window)
    t2 = datetime(2026, 10, 9, 14, 1, 0, tzinfo=timezone.utc)
    e2 = TransactionEvent(event_id="ooo-2", timestamp=t2, features=valid_features)
    flush2 = batcher.add_event(e2, partition=0, offset=1)
    assert flush2 is None  # Must NOT flush, belongs to same window

    # Manual flush yields both events in the same window
    meta, _ = batcher.flush_current_batch(persist=False)
    assert meta.received_count == 2
    assert meta.valid_count == 2
    assert meta.window_start == datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)


def test_late_event_arriving_after_window_flush_quarantined_without_mutating_history(valid_features):
    """Verify that an event arriving after its window has already flushed:
    1. Does NOT mutate the already persisted historical batch.
    2. Does NOT prematurely flush the active current window.
    3. Is quarantined with an explicit late-event diagnosis.
    4. Has its offset tracked and committed safely.
    """
    repo = SQLiteMonitoringRepository(db_path=":memory:")
    mon_svc = MonitoringService(repository=repo)
    batcher = MicroBatcher(batch_interval_seconds=300, max_batch_size=10, monitoring_service=mon_svc)

    # 1. Window 1 [14:00 - 14:05]: process e1 and flush
    t_w1 = datetime(2026, 10, 9, 14, 2, 0, tzinfo=timezone.utc)
    e1 = TransactionEvent(event_id="w1-clean", timestamp=t_w1, features=valid_features)
    batcher.add_event(e1, partition=0, offset=0)
    flush_w1 = batcher.flush_current_batch(persist=True)
    meta_w1, run_w1 = flush_w1
    assert meta_w1.valid_count == 1
    assert repo.count_runs() == 1

    # 2. Window 2 [14:05 - 14:10]: active and buffering
    t_w2 = datetime(2026, 10, 9, 14, 6, 0, tzinfo=timezone.utc)
    e2 = TransactionEvent(event_id="w2-clean", timestamp=t_w2, features=valid_features)
    batcher.add_event(e2, partition=0, offset=1)

    # 3. Delayed event belonging to Window 1 arrives NOW (after Window 1 is flushed)
    t_late = datetime(2026, 10, 9, 14, 4, 0, tzinfo=timezone.utc)
    e_late = TransactionEvent(event_id="w1-late", timestamp=t_late, features=valid_features)
    flush_late = batcher.add_event(e_late, partition=0, offset=2)

    # Active Window 2 must NOT be prematurely flushed
    assert flush_late is None

    # 4. Flush Window 2
    meta_w2, run_w2 = batcher.flush_current_batch(persist=True)

    # Window 2 contains the valid record e2 AND the quarantined late record e_late
    assert meta_w2.valid_count == 1
    assert meta_w2.quarantined_count == 1
    assert (0, 2) in meta_w2.tracked_offsets

    # Confirm Window 1 in repository was completely unmutated
    stored_w1 = repo.get_run(run_w1.run_id)
    assert stored_w1.sample_count == 1
    assert stored_w1.valid_sample_count == 1
    assert repo.count_runs() == 2
