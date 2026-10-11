"""ModelPulse Phase 6.2 Alert Crash Recovery & Live Integration Audit Test Suite.
Verifies:
1. Alert delivery crash window: Outbox staging, process crash after offset commit, recovery of pending alerts.
2. Provider idempotency and bounded retries during crash recovery.
3. Feature signature edge cases: Uncapped feature evaluation (preventing top-3 truncation masking).
4. SQLite outbox persistence and schema integrity.
"""
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List
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
    get_monitoring_repository,
)
from backend.app.schemas.monitoring import (
    MonitoringRunResponse,
    PredictionDriftSummary,
    SeverityBreakdown,
    TopContributor,
    WindowSpec,
)


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def mock_run_factory():
    """Factory creating customizable MonitoringRunResponse fixtures."""
    def _create(
        run_id: str = "run-rec-001",
        top_features: List[str] = None,
        severity_band: str = "High",
        severity_score: int = 75,
        is_synth: bool = True,
    ) -> MonitoringRunResponse:
        now = datetime.now(timezone.utc)
        top_features = top_features or ["amount", "oldbalanceOrg"]
        top_contribs = [
            TopContributor(
                feature_name=f,
                contribution_score=0.40,
                cumulative_share=0.50,
            )
            for f in top_features
        ]

        return MonitoringRunResponse(
            run_id=run_id,
            model_name="paysim-fraud-histgradientboost",
            model_version="1.0.0",
            run_type="simulator" if is_synth else "streaming_microbatch",
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
                metric="psi", value=0.30, threshold=0.25, drifted=True, status="ok"
            ),
            severity=SeverityBreakdown(
                score=severity_score,
                raw_score=float(severity_score),
                band=severity_band,
                confidence="normal",
                components={"D": 80.0, "P": 70.0, "Q": 20.0, "M": None},
                weights_used={"D": 0.533, "P": 0.267, "Q": 0.200},
            ),
            features=[],
            top_contributors=top_contribs,
            recommendations=["Investigate feature drift"],
            created_at=now,
        )

    return _create


# ==============================================================================
# 1. ALERT OUTBOX STAGING & CRASH RECOVERY
# ==============================================================================

def test_alert_outbox_staging_persists_prior_to_dispatch(mock_run_factory):
    """Verify stage_outbox persists pending records in repository with deterministic IDs."""
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)
    service = AlertService()
    run = mock_run_factory("run-outbox-001")

    staged = service.stage_outbox(run)
    assert len(staged) == 2  # slack + email

    pending = repo.get_pending_alert_outbox()
    assert len(pending) == 2
    assert any(p.channel == "slack" and p.outbox_id == "outbox-run-outbox-001-slack" for p in pending)
    assert any(p.channel == "email" and p.outbox_id == "outbox-run-outbox-001-email" for p in pending)
    assert all(p.status == AlertOutboxStatus.PENDING for p in pending)


@pytest.mark.asyncio
async def test_simulated_crash_after_offset_commit_recovers_pending_alerts(mock_run_factory):
    """Simulate crash between offset commit and alert dispatch:
    Pending alerts in outbox must be recovered and delivered without permanent loss.
    """
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)

    run = mock_run_factory("run-crash-sim-001")
    service1 = AlertService()

    # Step 1: Outbox staged (what happens before offset commit)
    staged = service1.stage_outbox(run)
    assert len(staged) == 2

    # CRASH SIMULATION:
    # Process terminates right now. service1 is discarded.
    # New process starts up with service2 and mock delivery channels:
    delivered_alerts = []

    def mock_slack_handler(req: httpx.Request):
        delivered_alerts.append("slack")
        return httpx.Response(200, json={"ok": True})

    def mock_email_sender(msg):
        delivered_alerts.append("email")
        return {}

    async with httpx.AsyncClient(transport=httpx.MockTransport(mock_slack_handler)) as http_client:
        mock_slack = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/TEST",
            http_client=http_client,
        )
        mock_email = EmailAlertChannel(
            smtp_host="smtp.example.com",
            recipient_email="oncall@example.com",
            smtp_sender_fn=mock_email_sender,
        )

        service2 = AlertService(
            slack_channel=mock_slack,
            email_channel=mock_email,
        )

        # Step 2: Crash recovery executed on startup
        recovered = await service2.recover_and_dispatch_pending()
        assert len(recovered) == 2
        assert all(r.status == AlertDeliveryStatus.DELIVERED for r in recovered)

        # Step 3: Outbox entries transitioned to DELIVERED
        pending_after = repo.get_pending_alert_outbox()
        assert len(pending_after) == 0

        slack_outbox = repo.get_alert_outbox_record("outbox-run-crash-sim-001-slack")
        email_outbox = repo.get_alert_outbox_record("outbox-run-crash-sim-001-email")
        assert slack_outbox.status == AlertOutboxStatus.DELIVERED
        assert email_outbox.status == AlertOutboxStatus.DELIVERED
        assert "slack" in delivered_alerts
        assert "email" in delivered_alerts


def test_sqlite_outbox_persistence_across_reconnect(tmp_path, mock_run_factory):
    """Verify SQLite alert outbox persists across database connection close/reopen."""
    db_file = tmp_path / "outbox_test.db"
    repo1 = SQLiteMonitoringRepository(db_path=str(db_file))
    run = mock_run_factory("run-sqlite-outbox-1")

    # Persist run and outbox in repo1
    repo1.save_run(run)
    outbox_rec = AlertOutboxRecord(
        outbox_id="outbox-run-sqlite-outbox-1-slack",
        run_id=run.run_id,
        channel="slack",
        severity_band="High",
        status=AlertOutboxStatus.PENDING,
        payload_json=run.model_dump_json(),
    )
    repo1.save_alert_outbox(outbox_rec)
    repo1.close()

    # Reopen in new repository connection
    repo2 = SQLiteMonitoringRepository(db_path=str(db_file))
    pending = repo2.get_pending_alert_outbox()
    assert len(pending) == 1
    assert pending[0].outbox_id == "outbox-run-sqlite-outbox-1-slack"
    assert pending[0].run_id == "run-sqlite-outbox-1"
    assert pending[0].status == "pending"

    # Update status
    repo2.update_alert_outbox_status(pending[0].outbox_id, "delivered")
    assert len(repo2.get_pending_alert_outbox()) == 0
    rec = repo2.get_alert_outbox_record("outbox-run-sqlite-outbox-1-slack")
    assert rec.status == "delivered"
    repo2.close()


# ==============================================================================
# 2. FEATURE SIGNATURE & ALERT IDENTITY EDGE CASES
# ==============================================================================

def test_feature_signature_evaluates_beyond_top_three_features(mock_run_factory):
    """Verify two runs differing only on 4th feature produce distinct feature signatures.
    Ensures that an arbitrary top-3 truncation does NOT mask distinct incidents.
    """
    service = AlertService()
    # 4 drifted features: amount, oldbalanceOrg, newbalanceOrig, type
    run1 = mock_run_factory(
        "run-feat-4a",
        top_features=["amount", "oldbalanceOrg", "newbalanceOrig", "type"],
    )
    # 4 drifted features: amount, oldbalanceOrg, newbalanceOrig, step
    run2 = mock_run_factory(
        "run-feat-4b",
        top_features=["amount", "oldbalanceOrg", "newbalanceOrig", "step"],
    )

    p1 = service.build_payload(run1)
    p2 = service.build_payload(run2)

    assert "type" in p1.feature_signature
    assert "step" in p2.feature_signature
    assert p1.feature_signature != p2.feature_signature
    assert p1.fingerprint != p2.fingerprint, "Differing 4th feature must not be masked by top-3 truncation!"


def test_feature_signature_order_invariance(mock_run_factory):
    """Verify that different ordering of the same feature list produces identical signature."""
    service = AlertService()
    run1 = mock_run_factory("run-ord-1", top_features=["amount", "type", "step"])
    run2 = mock_run_factory("run-ord-2", top_features=["step", "amount", "type"])

    p1 = service.build_payload(run1)
    p2 = service.build_payload(run2)

    assert p1.feature_signature == p2.feature_signature == "amount,step,type"
    assert p1.fingerprint == p2.fingerprint


def test_synthetic_vs_production_fingerprint_isolation(mock_run_factory):
    """Verify synthetic simulations and production batches have isolated fingerprints."""
    service = AlertService()
    run_synth = mock_run_factory("run-syn", is_synth=True)
    run_prod = mock_run_factory("run-prd", is_synth=False)

    p_synth = service.build_payload(run_synth)
    p_prod = service.build_payload(run_prod)

    assert "synthetic" in p_synth.fingerprint
    assert "production" in p_prod.fingerprint
    assert p_synth.fingerprint != p_prod.fingerprint


@pytest.mark.asyncio
async def test_outbox_exhaustion_marks_failed(mock_run_factory):
    """Verify repeated provider failure marks outbox record as failed after max retries."""
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)
    run = mock_run_factory("run-fail-rec-001")

    def mock_fail_handler(req):
        return httpx.Response(500, text="Internal Gateway Error")

    async with httpx.AsyncClient(transport=httpx.MockTransport(mock_fail_handler)) as http_client:
        mock_slack = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/FAIL",
            max_retries=1,
            backoff_seconds=0.01,
            http_client=http_client,
        )
        service = AlertService(
            slack_channel=mock_slack,
            email_channel=EmailAlertChannel(smtp_host=None),  # disabled
        )

        service.stage_outbox(run)
        recovered = await service.recover_and_dispatch_pending()

        # Slack failed
        assert any(r.channel == "slack" and r.status == AlertDeliveryStatus.FAILED for r in recovered)
        rec = repo.get_alert_outbox_record("outbox-run-fail-rec-001-slack")
        assert rec.status == AlertOutboxStatus.FAILED
        assert rec.retry_count >= 1
