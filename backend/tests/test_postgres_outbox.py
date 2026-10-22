
import asyncio
import uuid
from datetime import datetime, timezone, timedelta

import pytest

from backend.app.alerting.models import AlertOutboxStatus
from backend.app.alerting.service import AlertService
from backend.app.core.config import get_settings
from backend.app.db.postgres_repository import PostgreSQLMonitoringRepository
from backend.app.schemas.monitoring import (
    MonitoringRunResponse,
    PredictionDriftSummary,
    SeverityBreakdown,
    TopContributor,
    WindowSpec,
)


def make_postgres_test_run(run_id: str) -> MonitoringRunResponse:
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
            metric="psi", value=0.35, threshold=0.25,
            drifted=True, status="ok",
        ),
        severity=SeverityBreakdown(
            score=85,
            raw_score=85.0,
            band="High",
            confidence="normal",
            components={"D": 85.0, "P": 75.0, "Q": 20.0, "M": None},
            weights_used={"D": 0.533, "P": 0.267, "Q": 0.200},
        ),
        features=[],
        top_contributors=[
            TopContributor(
                feature_name="amount",
                contribution_score=0.5,
                cumulative_share=0.5,
            )
        ],
        recommendations=["PostgreSQL outbox fencing test"],
        created_at=now,
    )


@pytest.mark.asyncio
async def test_postgres_stale_worker_fencing():
    settings = get_settings()
    repo = PostgreSQLMonitoringRepository(settings.DATABASE_URL)
    run_id = f"pg-fencing-{uuid.uuid4().hex[:12]}"

    try:
        run = make_postgres_test_run(run_id)
        outbox_records = AlertService().build_outbox_records(run)[:1]
        assert len(outbox_records) == 1
        repo.save_run(run, outbox_records=outbox_records)

        worker_a = repo.claim_pending_alert_outbox(
            limit=1, worker_id="postgres-worker-A", lease_seconds=1
        )
        assert len(worker_a) == 1
        rec_a = worker_a[0]
        assert rec_a.claim_token == 1
        assert rec_a.status == AlertOutboxStatus.IN_FLIGHT

        await asyncio.sleep(1.2)

        worker_b = repo.claim_pending_alert_outbox(
            limit=1, worker_id="postgres-worker-B", lease_seconds=1
        )
        assert len(worker_b) == 1
        rec_b = worker_b[0]
        assert rec_b.outbox_id == rec_a.outbox_id
        assert rec_b.claim_token == 2
        assert rec_b.claimed_by == "postgres-worker-B"

        stale_update = repo.update_alert_outbox_status(
            rec_a.outbox_id,
            AlertOutboxStatus.DELIVERED,
            claim_token=rec_a.claim_token,
        )
        assert stale_update is False

        current = repo.get_alert_outbox_record(rec_b.outbox_id)
        assert current.status == AlertOutboxStatus.IN_FLIGHT
        assert current.claim_token == 2
        assert current.claimed_by == "postgres-worker-B"

        valid_update = repo.update_alert_outbox_status(
            rec_b.outbox_id,
            AlertOutboxStatus.DELIVERED,
            claim_token=rec_b.claim_token,
        )
        assert valid_update is True

        final_record = repo.get_alert_outbox_record(rec_b.outbox_id)
        assert final_record.status == AlertOutboxStatus.DELIVERED
        assert final_record.claim_token == 2

    finally:
        repo.delete_run(run_id)
        repo.close()



def test_postgres_failed_alert_reset_and_terminal_protection():
    settings = get_settings()
    repo = PostgreSQLMonitoringRepository(settings.DATABASE_URL)
    run_id = f"pg-reset-{uuid.uuid4().hex[:12]}"

    try:
        run = make_postgres_test_run(run_id)
        outbox_records = AlertService().build_outbox_records(run)[:1]
        repo.save_run(run, outbox_records=outbox_records)
        outbox_id = outbox_records[0].outbox_id

        # Mark the alert as failed.
        assert repo.update_alert_outbox_status(
            outbox_id,
            AlertOutboxStatus.FAILED,
            error_message="Simulated delivery failure",
            retry_count=3,
        )

        # Only failed alerts should be reset for replay.
        assert repo.reset_failed_outbox_record(outbox_id) is True

        reset_record = repo.get_alert_outbox_record(outbox_id)
        assert reset_record.status == AlertOutboxStatus.PENDING
        assert reset_record.retry_count == 0

        # Mark it delivered and verify terminal-state protection.
        assert repo.update_alert_outbox_status(
            outbox_id, AlertOutboxStatus.DELIVERED
        )
        assert repo.reset_failed_outbox_record(outbox_id) is False

        final_record = repo.get_alert_outbox_record(outbox_id)
        assert final_record.status == AlertOutboxStatus.DELIVERED

    finally:
        repo.delete_run(run_id)
        repo.close()
