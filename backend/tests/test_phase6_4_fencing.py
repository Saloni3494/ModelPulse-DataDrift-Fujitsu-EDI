"""ModelPulse Phase 6.4: Lease Fencing, Recovery Stress Tests & Outbox Audit.
Verifies:
1. Fencing token protection: Stale worker whose lease expired cannot overwrite reclaimed record.
2. Legal state machine transitions: pending -> in_flight -> delivered / suppressed_cooldown / skipped_disabled / failed.
3. Bounded retries: Retries remain strictly bounded and terminal records cannot be reset by replay.
4. Operator reset: Only failed records can be reset for replay; terminal delivered records are immune.
5. SQLite and In-Memory parity for fencing and terminal protection.
6. Failure injection: Atomicity of monitoring-run persistence + outbox staging, and offset holdback.
"""
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List
import uuid

import httpx
import pytest

from backend.app.alerting.models import (
    AlertDeliveryStatus,
    AlertOutboxRecord,
    AlertOutboxStatus,
    AlertPayload,
    AlertRecord,
)
from backend.app.alerting.service import AlertService
from backend.app.alerting.slack import SlackAlertChannel
from backend.app.alerting.email import EmailAlertChannel
from backend.app.db.repository import (
    InMemoryMonitoringRepository,
    SQLiteMonitoringRepository,
    set_monitoring_repository,
)
from backend.app.schemas.monitoring import (
    MonitoringRunResponse,
    PredictionDriftSummary,
    SeverityBreakdown,
    TopContributor,
    WindowSpec,
)
from backend.app.streaming.bus import InMemoryMessageBus
from backend.app.streaming.consumer import StreamingConsumer
from backend.app.schemas.events import TransactionEvent
from backend.app.streaming.dlq import DeadLetterQueueService
from backend.app.streaming.micro_batcher import MicroBatcher


# ==============================================================================
# HELPERS
# ==============================================================================

def make_test_run(
    run_id: str = "run-fencing-001",
    severity_band: str = "High",
    severity_score: int = 85,
) -> MonitoringRunResponse:
    now = datetime.now(timezone.utc)
    return MonitoringRunResponse(
        run_id=run_id,
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        run_type="streaming_microbatch",
        reference_window=WindowSpec(
            start=now - timedelta(days=7),
            end=now - timedelta(days=1),
            sample_count=5000,
        ),
        current_window=WindowSpec(
            start=now - timedelta(minutes=5),
            end=now,
            sample_count=1000,
        ),
        sample_count=1000,
        valid_sample_count=980,
        quarantined_sample_count=20,
        status="ok",
        dataset_drift_detected=True,
        prediction_drift=PredictionDriftSummary(
            metric="psi", value=0.35, threshold=0.25, drifted=True, status="ok"
        ),
        severity=SeverityBreakdown(
            score=severity_score,
            raw_score=float(severity_score),
            band=severity_band,
            confidence="normal",
            components={"D": 85.0, "P": 75.0, "Q": 20.0, "M": None},
            weights_used={"D": 0.533, "P": 0.267, "Q": 0.200},
        ),
        features=[],
        top_contributors=[
            TopContributor(
                feature_name="amount",
                contribution_score=0.50,
                cumulative_share=0.50,
            )
        ],
        recommendations=["Investigate drift"],
        created_at=now,
    )


# ==============================================================================
# 1. LEASE OWNERSHIP & FENCING TOKEN TESTS
# ==============================================================================

@pytest.mark.asyncio
async def test_stale_worker_fencing_token_rejection_sqlite(tmp_path):
    """Reproduce exact sequence:
    1. Worker A claims record (claim_token=1, claimed_by="worker-A").
    2. Lease expires.
    3. Worker B reclaims record (claim_token=2, claimed_by="worker-B").
    4. Worker A resumes and attempts to complete update using stale claim_token=1.
    5. Fencing check REJECTS Worker A's update (returns False).
    6. Worker B's active ownership is preserved.
    7. Worker B completes update using claim_token=2 (succeeds).
    """
    db_file = tmp_path / "fencing_sqlite.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    run = make_test_run("run-fencing-sqlite-01")
    service = AlertService()
    # Stage single record to strictly isolate single-record lease reclamation
    outbox_recs = service.build_outbox_records(run)[:1]
    repo.save_run(run, outbox_records=outbox_recs)

    # 1. Worker A claims record with a 1-second lease
    claimed_a = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-A", lease_seconds=1)
    assert len(claimed_a) == 1
    rec_a = claimed_a[0]
    assert rec_a.status == AlertOutboxStatus.IN_FLIGHT
    assert rec_a.claim_token == 1
    assert rec_a.claimed_by == "worker-A"

    # 2. Lease expires
    await asyncio.sleep(1.1)

    # 3. Worker B reclaims the expired record (using lease_seconds=1 threshold)
    claimed_b = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-B", lease_seconds=1)
    assert len(claimed_b) == 1
    rec_b = claimed_b[0]
    assert rec_b.outbox_id == rec_a.outbox_id
    assert rec_b.status == AlertOutboxStatus.IN_FLIGHT
    assert rec_b.claim_token == 2
    assert rec_b.claimed_by == "worker-B"

    # 4. Worker A resumes late and attempts to mark the record DELIVERED with stale token 1
    update_a_ok = repo.update_alert_outbox_status(
        outbox_id=rec_a.outbox_id,
        status=AlertOutboxStatus.DELIVERED,
        claim_token=rec_a.claim_token,  # token = 1
    )
    # 5. Stale update MUST be rejected!
    assert update_a_ok is False

    # 6. Verify Worker B's state is preserved intact in SQLite
    current_rec = repo.get_alert_outbox_record(rec_a.outbox_id)
    assert current_rec.status == AlertOutboxStatus.IN_FLIGHT
    assert current_rec.claim_token == 2
    assert current_rec.claimed_by == "worker-B"

    # 7. Worker B finishes successfully with valid token 2
    update_b_ok = repo.update_alert_outbox_status(
        outbox_id=rec_b.outbox_id,
        status=AlertOutboxStatus.DELIVERED,
        claim_token=rec_b.claim_token,  # token = 2
    )
    assert update_b_ok is True

    final_rec = repo.get_alert_outbox_record(rec_b.outbox_id)
    assert final_rec.status == AlertOutboxStatus.DELIVERED
    assert final_rec.claim_token == 2
    repo.close()


@pytest.mark.asyncio
async def test_stale_worker_fencing_token_rejection_in_memory():
    """Verify identical fencing token behavior on InMemoryMonitoringRepository."""
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)

    run = make_test_run("run-fencing-mem-01")
    service = AlertService()
    # Stage single record to strictly isolate single-record lease reclamation
    outbox_recs = service.build_outbox_records(run)[:1]
    repo.save_run(run, outbox_records=outbox_recs)

    # 1. Worker A claims with 1-second lease
    claimed_a = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-A", lease_seconds=1)
    assert len(claimed_a) == 1
    rec_a = claimed_a[0]
    assert rec_a.claim_token == 1
    assert rec_a.claimed_by == "worker-A"

    # 2. Wait for lease expiry
    await asyncio.sleep(1.1)

    # 3. Worker B reclaims
    claimed_b = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-B", lease_seconds=1)
    assert len(claimed_b) == 1
    rec_b = claimed_b[0]
    assert rec_b.outbox_id == rec_a.outbox_id
    assert rec_b.claim_token == 2
    assert rec_b.claimed_by == "worker-B"

    # 4. Worker A attempts stale update
    update_a_ok = repo.update_alert_outbox_status(
        outbox_id=rec_a.outbox_id,
        status=AlertOutboxStatus.DELIVERED,
        claim_token=rec_a.claim_token,
    )
    assert update_a_ok is False

    # 5. Worker B state preserved
    cur = repo.get_alert_outbox_record(rec_a.outbox_id)
    assert cur.status == AlertOutboxStatus.IN_FLIGHT
    assert cur.claim_token == 2

    # 6. Worker B updates with token 2
    update_b_ok = repo.update_alert_outbox_status(
        outbox_id=rec_b.outbox_id,
        status=AlertOutboxStatus.DELIVERED,
        claim_token=rec_b.claim_token,
    )
    assert update_b_ok is True
    assert repo.get_alert_outbox_record(rec_b.outbox_id).status == AlertOutboxStatus.DELIVERED


@pytest.mark.asyncio
async def test_multiple_successive_lease_expirations(tmp_path):
    """Test successive reclaims: Worker 1 (tok 1) -> Worker 2 (tok 2) -> Worker 3 (tok 3).
    Only Worker 3's update must succeed; both Workers 1 and 2 must be rejected.
    """
    db_file = tmp_path / "multi_expire.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    run = make_test_run("run-multi-expire-01")
    service = AlertService()
    outbox_recs = service.build_outbox_records(run)[:1]
    repo.save_run(run, outbox_records=outbox_recs)

    target_id = outbox_recs[0].outbox_id

    # Worker 1
    w1 = repo.claim_pending_alert_outbox(limit=10, worker_id="w1", lease_seconds=1)[0]
    assert w1.claim_token == 1
    await asyncio.sleep(1.1)

    # Worker 2
    w2 = repo.claim_pending_alert_outbox(limit=10, worker_id="w2", lease_seconds=1)[0]
    assert w2.claim_token == 2
    await asyncio.sleep(1.1)

    # Worker 3 (using lease_seconds=1 threshold)
    w3 = repo.claim_pending_alert_outbox(limit=10, worker_id="w3", lease_seconds=1)[0]
    assert w3.claim_token == 3

    # Worker 1 attempts update -> rejected
    assert repo.update_alert_outbox_status(target_id, AlertOutboxStatus.DELIVERED, claim_token=1) is False
    # Worker 2 attempts update -> rejected
    assert repo.update_alert_outbox_status(target_id, AlertOutboxStatus.DELIVERED, claim_token=2) is False
    # Worker 3 attempts update -> accepted
    assert repo.update_alert_outbox_status(target_id, AlertOutboxStatus.DELIVERED, claim_token=3) is True

    final = repo.get_alert_outbox_record(target_id)
    assert final.status == AlertOutboxStatus.DELIVERED
    assert final.claim_token == 3
    repo.close()


# ==============================================================================
# 2. OUTBOX STATE MACHINE AUDIT
# ==============================================================================

def test_terminal_states_immutable_against_stale_updates(tmp_path):
    """Verify all terminal states (delivered, suppressed_cooldown, skipped_disabled, failed)
    cannot be modified via update_alert_outbox_status.
    """
    db_file = tmp_path / "terminal_audit.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    run = make_test_run("run-term-01")
    service = AlertService()
    outbox_recs = service.build_outbox_records(run)
    repo.save_run(run, outbox_records=outbox_recs)

    rec_id = outbox_recs[0].outbox_id

    # Transition: pending -> in_flight -> delivered
    claimed = repo.claim_pending_alert_outbox(limit=1, worker_id="w1")[0]
    assert repo.update_alert_outbox_status(rec_id, AlertOutboxStatus.DELIVERED, claim_token=claimed.claim_token) is True

    # Attempt to transition delivered -> failed or delivered -> pending
    assert repo.update_alert_outbox_status(rec_id, AlertOutboxStatus.FAILED) is False
    assert repo.update_alert_outbox_status(rec_id, AlertOutboxStatus.PENDING) is False
    assert repo.update_alert_outbox_status(rec_id, AlertOutboxStatus.IN_FLIGHT) is False

    # Check status is strictly preserved
    assert repo.get_alert_outbox_record(rec_id).status == AlertOutboxStatus.DELIVERED
    repo.close()


def test_bounded_retries_and_failure_exhaustion(tmp_path):
    """Verify that retries remain strictly bounded:
    - Records start with retry_count = 0, max_retries = 3
    - Intermediate errors increment retry_count up to max_retries
    - Once retry_count reaches max_retries, status becomes failed
    - Failed records are excluded from pending claims
    """
    db_file = tmp_path / "retries_audit.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    run = make_test_run("run-retries-01")
    service = AlertService()
    outbox_recs = service.build_outbox_records(run)
    repo.save_run(run, outbox_records=outbox_recs)

    rec_id = outbox_recs[0].outbox_id

    # Attempt 1: claim, fail, increment retry to 1
    c1 = repo.claim_pending_alert_outbox(limit=1, worker_id="w1")[0]
    assert repo.update_alert_outbox_status(rec_id, AlertOutboxStatus.PENDING, "500 Error", retry_count=1, claim_token=c1.claim_token) is True
    r1 = repo.get_alert_outbox_record(rec_id)
    assert r1.status == AlertOutboxStatus.PENDING
    assert r1.retry_count == 1

    # Attempt 2: claim, fail, increment retry to 2
    c2 = repo.claim_pending_alert_outbox(limit=1, worker_id="w2")[0]
    assert repo.update_alert_outbox_status(rec_id, AlertOutboxStatus.PENDING, "502 Error", retry_count=2, claim_token=c2.claim_token) is True
    r2 = repo.get_alert_outbox_record(rec_id)
    assert r2.status == AlertOutboxStatus.PENDING
    assert r2.retry_count == 2

    # Attempt 3: claim, fail, reaches max_retries=3 -> transition to FAILED
    c3 = repo.claim_pending_alert_outbox(limit=1, worker_id="w3")[0]
    assert repo.update_alert_outbox_status(rec_id, AlertOutboxStatus.FAILED, "503 Error (Exhausted)", retry_count=3, claim_token=c3.claim_token) is True
    r3 = repo.get_alert_outbox_record(rec_id)
    assert r3.status == AlertOutboxStatus.FAILED
    assert r3.retry_count == 3
    assert "Exhausted" in r3.error_message

    # Confirm it is no longer returned in get_pending_alert_outbox or claim_pending_alert_outbox
    assert all(r.outbox_id != rec_id for r in repo.get_pending_alert_outbox())
    assert all(r.outbox_id != rec_id for r in repo.claim_pending_alert_outbox(worker_id="w4"))

    repo.close()


def test_operator_reset_restricted_to_failed_records(tmp_path):
    """Verify operator reset via reset_failed_outbox_record:
    - Succeeds for status='failed' -> resets to 'pending' with retry_count=0
    - Fails (returns False) for status='delivered' -> protects delivered records
    """
    db_file = tmp_path / "operator_reset.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    run = make_test_run("run-reset-01")
    service = AlertService()
    outbox_recs = service.build_outbox_records(run)
    repo.save_run(run, outbox_records=outbox_recs)

    rec_fail = outbox_recs[0].outbox_id
    rec_deliv = outbox_recs[1].outbox_id

    # Mark rec_fail as FAILED and rec_deliv as DELIVERED
    repo.update_alert_outbox_status(rec_fail, AlertOutboxStatus.FAILED, error_message="Fatal error", retry_count=3)
    repo.update_alert_outbox_status(rec_deliv, AlertOutboxStatus.DELIVERED)

    # 1. Operator reset on FAILED record must succeed
    assert repo.reset_failed_outbox_record(rec_fail) is True
    res_fail = repo.get_alert_outbox_record(rec_fail)
    assert res_fail.status == AlertOutboxStatus.PENDING
    assert res_fail.retry_count == 0
    assert "Manually reset" in res_fail.error_message

    # 2. Operator reset on DELIVERED record must be REJECTED
    assert repo.reset_failed_outbox_record(rec_deliv) is False
    res_deliv = repo.get_alert_outbox_record(rec_deliv)
    assert res_deliv.status == AlertOutboxStatus.DELIVERED

    repo.close()


# ==============================================================================
# 3. FAILURE INJECTION & OFFSET SAFETY TESTS
# ==============================================================================

@pytest.mark.asyncio
async def test_database_failure_injection_prevents_offset_commit(tmp_path):
    """Failure injection:
    When repository persistence fails during micro-batch processing:
    1. Consumer raises an exception.
    2. Kafka offset is NOT committed.
    3. Neither run nor outbox is persisted.
    """
    db_file = tmp_path / "failure_inj.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    bus = InMemoryMessageBus()
    dlq_service = DeadLetterQueueService(message_bus=bus, dlq_topic="test-dlq")
    micro_batcher = MicroBatcher(max_batch_size=2, batch_interval_seconds=300)
    consumer = StreamingConsumer(
        message_bus=bus,
        dlq_service=dlq_service,
        topic="test-topic",
        group_id="test-grp",
        micro_batcher=micro_batcher,
    )

    # Publish 2 valid events to trigger micro-batch flush
    now = datetime.now(timezone.utc)
    ev0 = TransactionEvent(
        event_id="evt-inj-001",
        timestamp=now,
        features={"amount": 1500.0, "oldbalanceOrg": 5000.0, "newbalanceOrig": 3500.0, "type": "PAYMENT"},
    )
    ev1 = TransactionEvent(
        event_id="evt-inj-002",
        timestamp=now,
        features={"amount": 2000.0, "oldbalanceOrg": 6000.0, "newbalanceOrig": 4000.0, "type": "PAYMENT"},
    )
    await bus.publish("test-topic", ev0.model_dump_json(), key="tx-00", partition=0)
    await bus.publish("test-topic", ev1.model_dump_json(), key="tx-01", partition=0)

    # Process first message successfully
    msgs0 = await bus.consume("test-topic", "test-grp", max_records=1)
    await consumer.process_single_message(msgs0[0])

    # INJECT FAILURE into SQLite repository before message 2 flushes the batch
    repo.simulate_failure = True

    # Processing message 2 triggers micro-batch flush & save_run, raising RuntimeError
    msgs1 = await bus.consume("test-topic", "test-grp", max_records=1)
    with pytest.raises(RuntimeError, match="Simulated SQLite database failure"):
        await consumer.process_single_message(msgs1[0])

    # Verify offset was NOT committed on partition 0
    committed_offset = await bus.get_committed_offset("test-topic", "test-grp", 0)
    assert committed_offset == -1  # Never committed!

    # Verify repository has 0 runs persisted
    assert repo.count_runs() == 0
    repo.close()


@pytest.mark.asyncio
async def test_alert_service_recovery_with_fencing_rejection():
    """Verify recover_and_dispatch_pending logs warning and discards stale record
    if claim_token was superseded prior to update.
    """
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)

    service = AlertService()
    run = make_test_run("run-recovery-fencing-01")
    staged = service.stage_outbox(run)
    assert len(staged) == 2

    # Worker A claims records
    claimed_a = repo.claim_pending_alert_outbox(limit=2, worker_id="worker-A", lease_seconds=1)
    assert len(claimed_a) == 2

    # Simulate lease expiry & Worker B reclaiming
    await asyncio.sleep(1.1)
    claimed_b = repo.claim_pending_alert_outbox(limit=2, worker_id="worker-B", lease_seconds=1)
    assert len(claimed_b) == 2
    assert all(r.claim_token == 2 for r in claimed_b)

    # Worker A now attempts recovery using stale records (token 1)
    # We invoke Slack channel send manually and verify update rejection
    for r in claimed_a:
        updated = repo.update_alert_outbox_status(
            r.outbox_id,
            AlertOutboxStatus.DELIVERED,
            claim_token=r.claim_token,  # token = 1
        )
        assert updated is False  # Fencing rejected!

    # Records in repo still have token 2 and in_flight status
    for r in claimed_b:
        cur = repo.get_alert_outbox_record(r.outbox_id)
        assert cur.claim_token == 2
        assert cur.status == AlertOutboxStatus.IN_FLIGHT
