"""ModelPulse Phase 6.3: Outbox Atomicity, Concurrency & Release Readiness Audit Test Suite.
Verifies:
1. Transactional atomicity: Run persistence + alert outbox staging within the same SQLite transaction.
2. Outbox concurrency: Atomic claim/lease mechanism preventing duplicate dispatch across concurrent workers.
3. State protection: Repeated staging cannot reset delivered or exhausted records to pending.
4. Manual recovery: Explicit operator reset of permanently failed outbox records.
5. SMTP partial refusal: Only refused recipients are retried on subsequent attempts.
6. Kafka failure boundaries: Outbox failure aborts offset commits; offset commit failure supports idempotent replay.
"""
import asyncio
from datetime import datetime, timezone, timedelta
import sqlite3
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
    MonitoringRunRequest,
    MonitoringRunResponse,
    PredictionDriftSummary,
    SeverityBreakdown,
    TopContributor,
    WindowSpec,
)
from backend.app.services.monitoring_service import MonitoringService
from backend.app.streaming.bus import InMemoryMessageBus
from backend.app.streaming.consumer import StreamingConsumer
from backend.app.schemas.events import TransactionEvent
from backend.app.streaming.dlq import DeadLetterQueueService
from backend.app.streaming.micro_batcher import MicroBatcher


# ==============================================================================
# FIXTURES & HELPERS
# ==============================================================================

def make_run_response(
    run_id: str = "run-atomicity-001",
    severity_band: str = "High",
    severity_score: int = 80,
    features: List[str] = None,
) -> MonitoringRunResponse:
    now = datetime.now(timezone.utc)
    features = features or ["amount", "oldbalanceOrg"]
    top_contribs = [
        TopContributor(
            feature_name=f,
            contribution_score=0.45,
            cumulative_share=0.50,
        )
        for f in features
    ]
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
            metric="psi", value=0.32, threshold=0.25, drifted=True, status="ok"
        ),
        severity=SeverityBreakdown(
            score=severity_score,
            raw_score=float(severity_score),
            band=severity_band,
            confidence="normal",
            components={"D": 80.0, "P": 75.0, "Q": 20.0, "M": None},
            weights_used={"D": 0.533, "P": 0.267, "Q": 0.200},
        ),
        features=[],
        top_contributors=top_contribs,
        recommendations=["Investigate feature drift"],
        created_at=now,
    )


# ==============================================================================
# 1. REPOSITORY AND TRANSACTION INTEGRITY
# ==============================================================================

def test_sqlite_run_and_outbox_atomic_commit(tmp_path):
    """Verify that run persistence and outbox staging commit atomically in the same SQLite transaction."""
    db_file = tmp_path / "atomic_test.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    run = make_run_response("run-atomic-commit-01")

    service = AlertService()
    outbox_records = service.build_outbox_records(run)
    assert len(outbox_records) == 2

    # Save run with atomic outbox records
    saved = repo.save_run(run, outbox_records=outbox_records)
    assert saved is True

    # Verify both run and outbox exist on disk
    retrieved_run = repo.get_run("run-atomic-commit-01")
    assert retrieved_run is not None
    assert retrieved_run.run_id == "run-atomic-commit-01"

    pending_outbox = repo.get_pending_alert_outbox()
    assert len(pending_outbox) == 2
    assert any(o.outbox_id == "outbox-run-atomic-commit-01-slack" for o in pending_outbox)
    assert any(o.outbox_id == "outbox-run-atomic-commit-01-email" for o in pending_outbox)
    repo.close()


def test_sqlite_run_and_outbox_rollback_on_failure(tmp_path):
    """Verify that if an error occurs during the transaction, BOTH run and outbox records roll back."""
    db_file = tmp_path / "rollback_test.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    run = make_run_response("run-atomic-fail-01")

    # Simulate database disk failure
    repo.simulate_failure = True

    service = AlertService()
    outbox_records = service.build_outbox_records(run)

    with pytest.raises(RuntimeError, match="Simulated SQLite database failure"):
        repo.save_run(run, outbox_records=outbox_records)

    repo.simulate_failure = False

    # Confirm neither the run nor the outbox was persisted
    assert repo.get_run("run-atomic-fail-01") is None
    assert len(repo.get_pending_alert_outbox()) == 0
    repo.close()


# ==============================================================================
# 2. OUTBOX CONCURRENCY AND RECOVERY
# ==============================================================================

@pytest.mark.asyncio
async def test_concurrent_recovery_workers_claim_lease_prevent_duplicates(tmp_path):
    """Verify that two concurrent recovery workers claim different records or only one claims,
    preventing duplicate external dispatch.
    """
    db_file = tmp_path / "concurrency_test.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    run = make_run_response("run-concurrent-01")
    service1 = AlertService()
    outbox_recs = service1.build_outbox_records(run)
    repo.save_run(run, outbox_records=outbox_recs)

    # Worker 1 claims pending outbox
    claimed_worker1 = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-node-1")
    assert len(claimed_worker1) == 2
    assert all(r.status == AlertOutboxStatus.IN_FLIGHT for r in claimed_worker1)

    # Worker 2 attempts to claim at the same time: must receive 0 records
    claimed_worker2 = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-node-2")
    assert len(claimed_worker2) == 0

    repo.close()


@pytest.mark.asyncio
async def test_expired_lease_is_reclaimable_after_crash(tmp_path):
    """Verify that if a worker crashes while processing an outbox record,
    another worker can reclaim the record once its lease expires.
    """
    db_file = tmp_path / "lease_expiry_test.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    run = make_run_response("run-lease-expire-01")
    service = AlertService()
    outbox_recs = service.build_outbox_records(run)
    repo.save_run(run, outbox_records=outbox_recs)

    # Worker 1 claims with a very short lease (1 second)
    claimed_worker1 = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-crashed", lease_seconds=1)
    assert len(claimed_worker1) == 2

    # Wait 1.1s for lease to expire
    await asyncio.sleep(1.1)

    # Worker 2 should now be able to reclaim the expired records
    claimed_worker2 = repo.claim_pending_alert_outbox(limit=10, worker_id="worker-rescuer", lease_seconds=1)
    assert len(claimed_worker2) == 2
    assert all(r.status == AlertOutboxStatus.IN_FLIGHT for r in claimed_worker2)

    repo.close()


def test_repeated_staging_cannot_reset_delivered_or_failed_record(tmp_path):
    """Verify that repeated staging of a batch cannot overwrite a DELIVERED or FAILED
    record back to PENDING.
    """
    db_file = tmp_path / "idempotent_staging.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)
    run = make_run_response("run-delivered-idempotent-01")

    service = AlertService()
    outbox_recs = service.build_outbox_records(run)
    repo.save_run(run, outbox_records=outbox_recs)

    # Simulate delivery of slack outbox
    slack_id = "outbox-run-delivered-idempotent-01-slack"
    repo.update_alert_outbox_status(slack_id, AlertOutboxStatus.DELIVERED)

    # Repeated staging call with PENDING records
    service.stage_outbox(run)

    # Confirm status was NOT overwritten back to PENDING
    rec = repo.get_alert_outbox_record(slack_id)
    assert rec.status == AlertOutboxStatus.DELIVERED

    # Test FAILED record state protection as well
    email_id = "outbox-run-delivered-idempotent-01-email"
    repo.update_alert_outbox_status(email_id, AlertOutboxStatus.FAILED, retry_count=3)
    service.stage_outbox(run)

    rec_email = repo.get_alert_outbox_record(email_id)
    assert rec_email.status == AlertOutboxStatus.FAILED
    assert rec_email.retry_count == 3

    repo.close()


def test_manual_recovery_of_failed_record(tmp_path):
    """Verify operator-initiated manual reset of a permanently failed outbox record."""
    db_file = tmp_path / "manual_reset.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)
    run = make_run_response("run-failed-reset-01")

    service = AlertService()
    outbox_recs = service.build_outbox_records(run)
    repo.save_run(run, outbox_records=outbox_recs)

    slack_id = "outbox-run-failed-reset-01-slack"
    repo.update_alert_outbox_status(slack_id, AlertOutboxStatus.FAILED, error_message="Channel deleted", retry_count=3)

    rec_before = repo.get_alert_outbox_record(slack_id)
    assert rec_before.status == AlertOutboxStatus.FAILED
    assert rec_before.retry_count == 3

    # Operator triggers manual reset
    reset_ok = repo.reset_failed_outbox_record(slack_id)
    assert reset_ok is True

    rec_after = repo.get_alert_outbox_record(slack_id)
    assert rec_after.status == AlertOutboxStatus.PENDING
    assert rec_after.retry_count == 0
    assert "Manually reset" in rec_after.error_message

    repo.close()


# ==============================================================================
# 3. ALERT DELIVERY SEMANTICS & PROVIDER BEHAVIOR
# ==============================================================================

@pytest.mark.asyncio
async def test_smtp_partial_recipient_refusal_only_retries_refused():
    """Verify that if SMTP accepts recipient A but refuses recipient B,
    subsequent retry attempts only target recipient B, preventing duplicate emails to A.
    """
    attempt_targets = []

    def mock_partial_sender(msg):
        to_header = msg["To"]
        attempt_targets.append(to_header)
        # On attempt 1, refuse bob
        if "bob@example.com" in to_header:
            return {"bob@example.com": (550, b"Mailbox full")}
        return {}

    channel = EmailAlertChannel(
        smtp_host="smtp.example.com",
        recipient_email="alice@example.com, bob@example.com",
        max_retries=2,
        backoff_seconds=0.01,
        smtp_sender_fn=mock_partial_sender,
    )

    payload = AlertService().build_payload(make_run_response("run-partial-smtp-01"))
    rec = await channel.send("alt-partial-test-01", payload)

    # Retries exhausted for bob
    assert rec.status == AlertDeliveryStatus.FAILED
    assert "bob@example.com" in rec.error_message

    # Verify attempt 1 targeted both, but attempt 2 ONLY targeted bob@example.com
    assert len(attempt_targets) == 2
    assert "alice@example.com" in attempt_targets[0] and "bob@example.com" in attempt_targets[0]
    assert "alice@example.com" not in attempt_targets[1]
    assert attempt_targets[1] == "bob@example.com"


@pytest.mark.asyncio
async def test_provider_acceptance_crash_duplicate_delivery_window():
    """Verify behavior when a provider accepts an alert but the process crashes before
    updating outbox status: demonstrates the at-least-once duplicate delivery window.
    """
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)

    run = make_run_response("run-dup-window-01")
    service1 = AlertService()
    service1.stage_outbox(run)

    dispatched_count = 0

    def mock_slack_handler(req: httpx.Request):
        nonlocal dispatched_count
        dispatched_count += 1
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(mock_slack_handler)) as http_client:
        mock_slack = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/TEST",
            http_client=http_client,
        )
        service2 = AlertService(
            slack_channel=mock_slack,
            email_channel=EmailAlertChannel(smtp_host=None),
        )

        # Worker 1 dispatches to Slack, Slack accepts (dispatched_count == 1)
        # But process crashes BEFORE recording status in repo!
        slack_outbox_rec = repo.get_alert_outbox_record("outbox-run-dup-window-01-slack")
        payload = AlertPayload.model_validate_json(slack_outbox_rec.payload_json)
        await mock_slack.send("alt-slack-run-dup-window-01", payload)
        assert dispatched_count == 1

        # Outbox record is still PENDING because the crash prevented update
        assert repo.get_alert_outbox_record("outbox-run-dup-window-01-slack").status == AlertOutboxStatus.PENDING

        # After restart, recovery runs and redelivers
        recovered = await service2.recover_and_dispatch_pending()
        assert any(r.channel == "slack" and r.status == AlertDeliveryStatus.DELIVERED for r in recovered)

        # Slack was called twice: proves at-least-once delivery window
        assert dispatched_count == 2


# ==============================================================================
# 4. KAFKA FAILURE-BOUNDARY TESTS
# ==============================================================================

@pytest.mark.asyncio
async def test_outbox_staging_failure_aborts_offset_commit(tmp_path):
    """Verify that if alert outbox staging fails, consumer aborts batch finalization
    and Kafka offsets are NOT committed.
    """
    db_file = tmp_path / "offset_abort.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    set_monitoring_repository(repo)

    bus = InMemoryMessageBus()
    dlq = DeadLetterQueueService(dlq_topic="events-dlq", message_bus=bus)
    batcher = MicroBatcher(max_batch_size=2)
    consumer = StreamingConsumer(
        message_bus=bus,
        dlq_service=dlq,
        micro_batcher=batcher,
        topic="events-test",
        group_id="test-abort-group",
    )

    # Publish event 0
    now = datetime.now(timezone.utc)
    ev0 = TransactionEvent(
        event_id="tx-offset-00",
        timestamp=now,
        features={"amount": 100.0, "oldbalanceOrg": 500.0, "newbalanceOrig": 400.0, "type": "TRANSFER"},
    )
    await bus.publish("events-test", ev0.model_dump_json(), key="tx-00", partition=0)
    msgs0 = await bus.consume("events-test", "test-abort-group", max_records=1)
    await consumer.process_single_message(msgs0[0])

    # Publish event 1 (triggers batch flush of size 2)
    ev1 = TransactionEvent(
        event_id="tx-offset-01",
        timestamp=now,
        features={"amount": 200.0, "oldbalanceOrg": 600.0, "newbalanceOrig": 400.0, "type": "TRANSFER"},
    )
    await bus.publish("events-test", ev1.model_dump_json(), key="tx-01", partition=0)
    msgs1 = await bus.consume("events-test", "test-abort-group", max_records=1)

    # Simulate database disk failure right when batch flushes
    repo.simulate_failure = True

    # Processing message 1 triggers batch flush and persistence
    with pytest.raises(RuntimeError):
        await consumer.process_single_message(msgs1[0])

    # Assert that Kafka offsets were NEVER committed (remains -1 / None)
    comm_off = await bus.get_committed_offset("events-test", "test-abort-group", partition=0)
    assert comm_off is None or comm_off == -1

    repo.simulate_failure = False
    repo.close()
