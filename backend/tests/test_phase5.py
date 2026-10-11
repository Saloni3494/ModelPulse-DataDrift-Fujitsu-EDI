"""ModelPulse Phase 5 Streaming Test Suite.
Validates:
1. Versioned event contracts and schema validation.
2. Deterministic producer replay and reference immutability.
3. Five-minute event-time windowing and max-batch-size thresholding.
4. Out-of-order events and window transitions.
5. Deduplication and idempotent processing.
6. Dead Letter Queue (DLQ) routing and payload preservation.
7. Bounded retries and retry exhaustion.
8. Strict manual offset commit ordering (uncommitted on persistence failure).
9. All-quarantined and empty micro-batches.
10. Graceful shutdown.
11. End-to-end hand-checkable event accounting audit.
12. Optional live broker test (skipped when broker unavailable).
"""
import asyncio
from datetime import datetime, timezone, timedelta
import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import pytest

from backend.app.db.repository import InMemoryMonitoringRepository
from backend.app.schemas.events import (
    TransactionEvent,
    DLQEvent,
    MicroBatchMetadata,
    ReplaySummary,
)
from backend.app.services.model_service import ModelService
from backend.app.services.monitoring_service import MonitoringService
from backend.app.streaming.bus import (
    BusMessage,
    InMemoryMessageBus,
    set_message_bus,
)
from backend.app.streaming.dlq import DeadLetterQueueService
from backend.app.streaming.metrics import StreamingMetrics
from backend.app.streaming.micro_batcher import (
    MicroBatcher,
    align_to_window,
)
from backend.app.streaming.producer import (
    TransactionProducer,
    DeterministicReplayService,
)
from backend.app.streaming.consumer import StreamingConsumer


@pytest.fixture
def test_bus() -> InMemoryMessageBus:
    bus = InMemoryMessageBus()
    set_message_bus(bus)
    return bus


@pytest.fixture
def test_repo() -> InMemoryMonitoringRepository:
    return InMemoryMonitoringRepository()


@pytest.fixture
def valid_features() -> Dict[str, Any]:
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
# 1. EVENT SCHEMA VALIDATION & VERSION COMPATIBILITY
# ==============================================================================

def test_event_schema_validation_and_versions(valid_features):
    """Verify versioned TransactionEvent accepts supported version and rejects invalid envelopes."""
    # 1. Valid event
    evt = TransactionEvent(
        event_id="tx-100",
        schema_version="1.0.0",
        features=valid_features,
    )
    assert evt.event_id == "tx-100"
    assert evt.schema_version == "1.0.0"
    assert evt.features["amount"] == 9839.64

    # 2. Unsupported schema version
    with pytest.raises(ValueError, match="Unsupported schema version"):
        TransactionEvent(
            event_id="tx-101",
            schema_version="2.0.0",
            features=valid_features,
        )

    # 3. Empty event_id rejected
    with pytest.raises(ValueError, match="event_id must be a non-empty string"):
        TransactionEvent(
            event_id="   ",
            schema_version="1.0.0",
            features=valid_features,
        )


# ==============================================================================
# 2. DETERMINISTIC PRODUCER REPLAY & REFERENCE IMMUTABILITY
# ==============================================================================

@pytest.mark.asyncio
async def test_deterministic_producer_replay_and_reference_immutability():
    """Verify replay with fixed seed produces identical event streams without mutating reference."""
    bus1 = InMemoryMessageBus()
    prod1 = TransactionProducer(topic="events", message_bus=bus1)
    replay_svc1 = DeterministicReplayService(producer=prod1)

    ref_before = replay_svc1.get_reference_data()

    # Replay 40 events with seed 42
    summary1 = await replay_svc1.replay_scenario(
        scenario_id="numeric_mean_variance_shift",
        seed=42,
        n_samples=40,
    )

    ref_after = replay_svc1.get_reference_data()

    # Assert reference dataset immutability
    assert len(ref_before) == len(ref_after)
    assert ref_before.equals(ref_after)

    # Assert published counts
    assert summary1.published_count == 40
    assert summary1.failed_count == 0

    # Replay on second bus with same seed
    bus2 = InMemoryMessageBus()
    prod2 = TransactionProducer(topic="events", message_bus=bus2)
    replay_svc2 = DeterministicReplayService(producer=prod2)

    summary2 = await replay_svc2.replay_scenario(
        scenario_id="numeric_mean_variance_shift",
        seed=42,
        n_samples=40,
    )

    # Verify bit-for-bit equivalence of message values
    msgs1 = bus1.get_topic_messages("events")
    msgs2 = bus2.get_topic_messages("events")
    assert len(msgs1) == len(msgs2) == 40

    for m1, m2 in zip(msgs1, msgs2):
        d1 = json.loads(m1.value)
        d2 = json.loads(m2.value)
        assert d1["event_id"] == d2["event_id"]
        assert d1["features"] == d2["features"]
        assert d1["metadata"]["synthetic"] is True


# ==============================================================================
# 3. WINDOW ALIGNMENT & BOUNDARIES
# ==============================================================================

def test_five_minute_window_alignment_boundaries():
    """Verify alignment of UTC timestamps into strict 300-second event-time intervals."""
    t1 = datetime(2026, 10, 9, 14, 2, 15, tzinfo=timezone.utc)
    w_start1, w_end1 = align_to_window(t1, interval_seconds=300)
    assert w_start1 == datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)
    assert w_end1 == datetime(2026, 10, 9, 14, 5, 0, tzinfo=timezone.utc)

    # Boundary edge: exactly on 14:05:00
    t2 = datetime(2026, 10, 9, 14, 5, 0, tzinfo=timezone.utc)
    w_start2, w_end2 = align_to_window(t2, interval_seconds=300)
    assert w_start2 == datetime(2026, 10, 9, 14, 5, 0, tzinfo=timezone.utc)
    assert w_end2 == datetime(2026, 10, 9, 14, 10, 0, tzinfo=timezone.utc)

    # 14:04:59.999 falls in the earlier window
    t3 = datetime(2026, 10, 9, 14, 4, 59, 999000, tzinfo=timezone.utc)
    w_start3, w_end3 = align_to_window(t3, interval_seconds=300)
    assert w_start3 == w_start1
    assert w_end3 == w_end1


# ==============================================================================
# 4. MAX BATCH SIZE FLUSHING & WINDOW TRANSITIONS
# ==============================================================================

def test_max_batch_size_threshold_immediate_flush(valid_features, test_repo):
    """Verify micro-batch flushes immediately when max_batch_size is reached within the same window."""
    mon_svc = MonitoringService(repository=test_repo)
    batcher = MicroBatcher(
        batch_interval_seconds=300,
        max_batch_size=5,
        monitoring_service=mon_svc,
    )

    t0 = datetime(2026, 10, 9, 14, 1, 0, tzinfo=timezone.utc)
    res = None
    for i in range(5):
        evt = TransactionEvent(
            event_id=f"evt-{i}",
            timestamp=t0 + timedelta(seconds=i),
            schema_version="1.0.0",
            features=valid_features,
        )
        res = batcher.add_event(evt, partition=0, offset=i)

    # On the 5th event, flush must occur
    assert res is not None
    meta, run_resp = res
    assert meta.received_count == 5
    assert meta.valid_count == 5
    assert meta.source_offsets == {"0": 4}
    assert meta.status == "completed"


def test_window_transition_triggers_flush_of_prior_window(valid_features, test_repo):
    """Verify that an event arriving in a subsequent window triggers flush of the pending window."""
    mon_svc = MonitoringService(repository=test_repo)
    batcher = MicroBatcher(
        batch_interval_seconds=300,
        max_batch_size=100,
        monitoring_service=mon_svc,
    )

    # Window 1: 14:00 to 14:05
    t_win1 = datetime(2026, 10, 9, 14, 2, 0, tzinfo=timezone.utc)
    evt1 = TransactionEvent(event_id="evt-w1", timestamp=t_win1, features=valid_features)
    flush1 = batcher.add_event(evt1, partition=0, offset=0)
    assert flush1 is None  # Buffered

    # Window 2: 14:05 to 14:10
    t_win2 = datetime(2026, 10, 9, 14, 6, 0, tzinfo=timezone.utc)
    evt2 = TransactionEvent(event_id="evt-w2", timestamp=t_win2, features=valid_features)
    flush2 = batcher.add_event(evt2, partition=0, offset=1)

    # Window 1 was flushed!
    assert flush2 is not None
    meta, _ = flush2
    assert meta.window_start == datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)
    assert meta.received_count == 1
    assert meta.source_offsets == {"0": 0}


# ==============================================================================
# 5. DEDUPLICATION & IDEMPOTENT PROCESSING
# ==============================================================================

@pytest.mark.asyncio
async def test_duplicate_delivery_and_idempotent_processing(test_bus, test_repo, valid_features):
    """Verify duplicate event IDs are detected, tracked, and committed without double-counting."""
    prod = TransactionProducer(topic="events", message_bus=test_bus)
    dlq_svc = DeadLetterQueueService(dlq_topic="events.dlq", message_bus=test_bus)
    mon_svc = MonitoringService(repository=test_repo)
    metrics = StreamingMetrics()

    batcher = MicroBatcher(
        batch_interval_seconds=300,
        max_batch_size=10,
        monitoring_service=mon_svc,
    )
    consumer = StreamingConsumer(
        topic="events",
        group_id="test-group",
        message_bus=test_bus,
        dlq_service=dlq_svc,
        micro_batcher=batcher,
        metrics=metrics,
    )

    # Publish same event ID twice
    t0 = datetime(2026, 10, 9, 14, 1, 0, tzinfo=timezone.utc)
    evt1 = TransactionEvent(event_id="dup-unique-001", timestamp=t0, features=valid_features)
    evt2 = TransactionEvent(event_id="dup-unique-001", timestamp=t0, features=valid_features)

    await prod.publish_event(evt1)
    await prod.publish_event(evt2)

    n = await consumer.process_available_messages()
    assert n == 2

    # Flush pending batch
    flush_res = await consumer.flush_and_commit(persist=True)
    assert flush_res is not None
    meta, run_resp = flush_res

    assert meta.received_count == 2
    assert meta.valid_count == 1
    assert meta.duplicate_count == 1

    # Verify committed offset advanced to 1
    comm = await test_bus.get_committed_offset("events", "test-group", partition=0)
    assert comm == 1


# ==============================================================================
# 6. DLQ ROUTING & PAYLOAD INTEGRITY
# ==============================================================================

@pytest.mark.asyncio
async def test_malformed_and_unsupported_events_routed_to_dlq(test_bus, test_repo):
    """Verify malformed JSON, unsupported schema versions, and invalid envelopes route to DLQ."""
    prod = TransactionProducer(topic="events", message_bus=test_bus)
    dlq_svc = DeadLetterQueueService(dlq_topic="events.dlq", message_bus=test_bus)
    mon_svc = MonitoringService(repository=test_repo)
    batcher = MicroBatcher(monitoring_service=mon_svc)
    consumer = StreamingConsumer(
        topic="events",
        group_id="dlq-group",
        message_bus=test_bus,
        dlq_service=dlq_svc,
        micro_batcher=batcher,
    )

    # 1. Corrupted non-JSON string
    await prod.publish_raw("NOT_A_VALID_JSON_STRING{{{", key="corrupt-1")

    # 2. Unsupported schema version
    await prod.publish_raw(
        json.dumps({
            "event_id": "bad-version",
            "schema_version": "9.9.9",
            "features": {"amount": 100},
        }),
        key="corrupt-2",
    )

    # 3. Missing required features
    await prod.publish_raw(
        json.dumps({
            "event_id": "missing-features",
            "schema_version": "1.0.0",
        }),
        key="corrupt-3",
    )

    processed = await consumer.process_available_messages()
    assert processed == 3

    # All 3 messages must be in DLQ
    dlq_events = dlq_svc.get_sent_events()
    assert len(dlq_events) == 3

    assert dlq_events[0].failure_category == "malformed_json"
    assert dlq_events[0].original_payload == "NOT_A_VALID_JSON_STRING{{{"

    assert dlq_events[1].failure_category == "unsupported_schema_version"
    assert dlq_events[1].event_id == "bad-version"

    assert dlq_events[2].failure_category == "validation_error"
    assert dlq_events[2].event_id == "missing-features"

    # Offsets committed for all 3 because safely in DLQ
    comm = await test_bus.get_committed_offset("events", "dlq-group", partition=0)
    assert comm == 2


# ==============================================================================
# 7. BOUNDED RETRIES & RETRY EXHAUSTION
# ==============================================================================

@pytest.mark.asyncio
async def test_bounded_retries_and_retry_exhaustion_routes_to_dlq(test_bus, valid_features):
    """Verify transient failures retry up to max_retries before routing to DLQ."""
    prod = TransactionProducer(topic="events", message_bus=test_bus)
    dlq_svc = DeadLetterQueueService(dlq_topic="events.dlq", message_bus=test_bus)
    metrics = StreamingMetrics()

    # Mock batcher that always raises transient error
    mock_batcher = MagicMock()
    mock_batcher.add_event.side_effect = RuntimeError("Simulated transient socket lock")

    consumer = StreamingConsumer(
        topic="events",
        group_id="retry-group",
        message_bus=test_bus,
        dlq_service=dlq_svc,
        micro_batcher=mock_batcher,
        max_retries=2,
        retry_backoff_ms=50,  # Fast backoff for test
        metrics=metrics,
    )

    evt = TransactionEvent(event_id="retry-test-1", features=valid_features)
    await prod.publish_event(evt)

    await consumer.process_available_messages()

    # add_event called 3 times total (1 initial + 2 retries)
    assert mock_batcher.add_event.call_count == 3
    assert metrics.events_retried == 3
    assert metrics.retry_exhaustions == 1

    # Event routed to DLQ with category 'retry_exhausted'
    dlq_events = dlq_svc.get_sent_events()
    assert len(dlq_events) == 1
    assert dlq_events[0].failure_category == "retry_exhausted"
    assert dlq_events[0].attempt_count == 3
    assert dlq_events[0].event_id == "retry-test-1"

    # Offset safely committed once in DLQ
    comm = await test_bus.get_committed_offset("events", "retry-group", partition=0)
    assert comm == 0


# ==============================================================================
# 8. OFFSET COMMIT POLICY ON PERSISTENCE FAILURE
# ==============================================================================

@pytest.mark.asyncio
async def test_offsets_remain_uncommitted_on_persistence_failure(test_bus, valid_features):
    """CRITICAL: If database persistence fails, offsets MUST NOT be committed."""
    failing_repo = InMemoryMonitoringRepository()
    failing_repo.simulate_failure = True  # Inject DB outage

    mon_svc = MonitoringService(repository=failing_repo)
    batcher = MicroBatcher(monitoring_service=mon_svc)
    consumer = StreamingConsumer(
        topic="events",
        group_id="safe-offset-group",
        message_bus=test_bus,
        micro_batcher=batcher,
    )

    prod = TransactionProducer(topic="events", message_bus=test_bus)
    evt = TransactionEvent(event_id="persist-fail-evt", features=valid_features)
    await prod.publish_event(evt)

    await consumer.process_available_messages()

    # Attempt to flush and persist batch -> MUST RAISE EXCEPTION
    with pytest.raises(RuntimeError, match="Simulated database outage"):
        await consumer.flush_and_commit(persist=True)

    # Offsets must remain UNCOMMITTED (-1)
    comm = await test_bus.get_committed_offset("events", "safe-offset-group", partition=0)
    assert comm == -1


# ==============================================================================
# 9. ALL-QUARANTINED & EMPTY MICRO-BATCHES
# ==============================================================================

@pytest.mark.asyncio
async def test_all_quarantined_micro_batch_monitoring(test_bus, test_repo):
    """Verify micro-batch where all events are quarantined executes monitoring run with status all_quarantined."""
    mon_svc = MonitoringService(repository=test_repo)
    batcher = MicroBatcher(monitoring_service=mon_svc)
    consumer = StreamingConsumer(
        topic="events",
        group_id="all-quar-group",
        message_bus=test_bus,
        micro_batcher=batcher,
    )
    prod = TransactionProducer(topic="events", message_bus=test_bus)

    # Publish 10 records with negative amounts
    for i in range(10):
        corrupt_features = {
            "step": 1,
            "type": "TRANSFER",
            "amount": -100.0 * (i + 1),  # Quarantined
            "oldbalanceOrg": 500.0,
            "newbalanceOrig": 0.0,
            "oldbalanceDest": 0.0,
            "newbalanceDest": 500.0,
        }
        evt = TransactionEvent(event_id=f"quar-{i}", features=corrupt_features)
        await prod.publish_event(evt)

    await consumer.process_available_messages()
    flush_res = await consumer.flush_and_commit(persist=True)

    assert flush_res is not None
    meta, run_resp = flush_res

    assert meta.valid_count == 0
    assert meta.quarantined_count == 10
    assert meta.status == "all_quarantined"

    assert run_resp is not None
    assert run_resp.prediction_drift.status == "all_quarantined"
    assert run_resp.prediction_drift.value is None
    assert run_resp.severity.components["Q"] == 100.0
    assert run_resp.status == "quality_failure"


@pytest.mark.asyncio
async def test_empty_micro_batch_handling(test_bus, test_repo):
    """Verify flushing an empty micro-batch produces empty metadata without invoking monitoring."""
    mon_svc = MonitoringService(repository=test_repo)
    batcher = MicroBatcher(monitoring_service=mon_svc)
    consumer = StreamingConsumer(
        topic="events",
        group_id="empty-group",
        message_bus=test_bus,
        micro_batcher=batcher,
    )

    flush_res = await consumer.flush_and_commit(persist=True)
    assert flush_res is not None
    meta, run_resp = flush_res

    assert meta.status == "empty"
    assert meta.received_count == 0
    assert run_resp is None


# ==============================================================================
# 10. GRACEFUL SHUTDOWN
# ==============================================================================

@pytest.mark.asyncio
async def test_graceful_shutdown_flushes_pending_work(test_bus, test_repo, valid_features):
    """Verify consumer.stop() safely flushes pending micro-batch and commits offsets."""
    mon_svc = MonitoringService(repository=test_repo)
    batcher = MicroBatcher(monitoring_service=mon_svc)
    consumer = StreamingConsumer(
        topic="events",
        group_id="shutdown-group",
        message_bus=test_bus,
        micro_batcher=batcher,
    )
    prod = TransactionProducer(topic="events", message_bus=test_bus)

    evt = TransactionEvent(event_id="shutdown-evt", features=valid_features)
    await prod.publish_event(evt)
    await consumer.process_available_messages()

    # Invoke stop
    await consumer.stop()

    assert consumer._running is False
    batches = consumer.get_completed_batches()
    assert len(batches) == 1
    meta, run_resp = batches[0]
    assert meta.valid_count == 1

    # Offset safely committed
    comm = await test_bus.get_committed_offset("events", "shutdown-group", partition=0)
    assert comm == 0


# ==============================================================================
# 11. END-TO-END SYNTHETIC REPLAY ACCOUNTING AUDIT
# ==============================================================================

@pytest.mark.asyncio
async def test_end_to_end_synthetic_replay_exact_accounting_audit(test_bus, test_repo):
    """Hand-checkable end-to-end accounting audit:
    Published = Valid + Quarantined + DLQ + Duplicates + Failed
    Zero double counting.
    """
    prod = TransactionProducer(topic="events", message_bus=test_bus)
    dlq_svc = DeadLetterQueueService(dlq_topic="events.dlq", message_bus=test_bus)
    mon_svc = MonitoringService(repository=test_repo)
    replay_svc = DeterministicReplayService(producer=prod)
    metrics = StreamingMetrics()

    batcher = MicroBatcher(
        batch_interval_seconds=300,
        max_batch_size=1000,
        monitoring_service=mon_svc,
    )
    consumer = StreamingConsumer(
        topic="events",
        group_id="audit-group",
        message_bus=test_bus,
        dlq_service=dlq_svc,
        micro_batcher=batcher,
        metrics=metrics,
    )

    t0 = datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)

    # 1. Publish 80 clean events from scenario_no_drift
    summary_clean = await replay_svc.replay_scenario(
        scenario_id="no_drift",
        seed=42,
        n_samples=80,
        base_event_time=t0,
        time_step_seconds=1.0,
    )
    assert summary_clean.published_count == 80

    # 2. Publish 15 quarantined events (negative amounts)
    for i in range(15):
        corrupt_features = {
            "step": 1,
            "type": "PAYMENT",
            "amount": -50.0 * (i + 1),  # Negative amount -> Quarantined
            "oldbalanceOrg": 500.0,
            "newbalanceOrig": 500.0,
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
        }
        evt = TransactionEvent(
            event_id=f"quar-audit-{i}",
            timestamp=t0 + timedelta(seconds=80 + i),
            features=corrupt_features,
        )
        await prod.publish_event(evt)

    # 3. Publish 5 DLQ events (malformed payloads)
    # 2 unparseable JSON
    await prod.publish_raw("BAD_JSON_1{{{", key="dlq-1")
    await prod.publish_raw("BAD_JSON_2{{{", key="dlq-2")
    # 2 unsupported schema versions
    await prod.publish_raw(
        json.dumps({"event_id": "v2-1", "schema_version": "2.0.0", "features": {}}),
        key="dlq-3",
    )
    await prod.publish_raw(
        json.dumps({"event_id": "v2-2", "schema_version": "3.0.0", "features": {}}),
        key="dlq-4",
    )
    # 1 missing features
    await prod.publish_raw(
        json.dumps({"event_id": "no-feat", "schema_version": "1.0.0"}),
        key="dlq-5",
    )

    # 4. Publish 5 duplicate deliveries (duplicate IDs of already published events)
    for i in range(5):
        dup_evt = TransactionEvent(
            event_id=f"sim-no_drift-s42-{i:06d}",  # Re-sending identical ID from clean set
            timestamp=t0 + timedelta(seconds=95 + i),
            features={
                "step": 1,
                "type": "PAYMENT",
                "amount": 100.0,
                "oldbalanceOrg": 500.0,
                "newbalanceOrig": 400.0,
                "oldbalanceDest": 0.0,
                "newbalanceDest": 0.0,
            },
        )
        await prod.publish_event(dup_evt)

    total_published = 80 + 15 + 5 + 5  # Exactly 105 messages
    all_msgs = test_bus.get_topic_messages("events")
    assert len(all_msgs) == total_published == 105

    # Process all available messages through consumer
    n_processed = await consumer.process_available_messages(max_messages=200)
    assert n_processed == 105

    # Flush batch to complete monitoring run and commit offsets
    flush_res = await consumer.flush_and_commit(persist=True)
    assert flush_res is not None
    meta, run_resp = flush_res

    # Audit hand-checkable counts:
    # 1. Total ingested
    assert meta.received_count == 105
    # 2. Valid records
    assert meta.valid_count == 80
    # 3. Quarantined records
    assert meta.quarantined_count == 15
    # 4. DLQ records
    assert meta.dlq_count == 5
    # 5. Duplicates detected
    assert meta.duplicate_count == 5
    # 6. Unrecoverable failures
    assert meta.failed_count == 0

    # Verification of Exact Conservation Law:
    # Published = Valid + Quarantined + DLQ + Duplicates + Failed
    accounted_total = (
        meta.valid_count
        + meta.quarantined_count
        + meta.dlq_count
        + meta.duplicate_count
        + meta.failed_count
    )
    assert accounted_total == total_published == 105

    # DLQ service received exactly 5 events
    assert dlq_svc.count_sent_events() == 5

    # Offsets committed up to the last message (offset 104, 0-indexed)
    comm = await test_bus.get_committed_offset("events", "audit-group", partition=0)
    assert comm == 104

    # Run persisted to repository
    assert run_resp is not None
    assert test_repo.count_runs() == 1


# ==============================================================================
# 12. OPTIONAL LIVE KAFKA INTEGRATION TEST
# ==============================================================================

@pytest.mark.asyncio
async def test_live_kafka_broker_integration():
    """Optional integration test: connects to live Kafka broker if available, otherwise skips."""
    import socket
    s = socket.socket()
    try:
        s.settimeout(0.5)
        s.connect(("localhost", 9092))
        s.close()
    except Exception:
        pytest.skip("Local Kafka broker not reachable on localhost:9092 (optional test)")

    from backend.app.streaming.bus import AIOKafkaMessageBus
    bus = AIOKafkaMessageBus(bootstrap_servers="localhost:9092")
    try:
        off = await bus.publish(topic="test-ping", value="ping-test")
        assert off >= 0
    finally:
        await bus.close()
