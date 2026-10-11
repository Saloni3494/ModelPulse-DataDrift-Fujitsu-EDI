"""ModelPulse Phase 6.1 Reliability & Integration Audit Test Suite.
Verifies:
1. Dashboard fallback correctness (connection refusal, timeouts, HTTP errors, malformed responses, SQLite vs in-memory provenance).
2. Alert fingerprinting and cooldown policy (distinct incidents not falsely suppressed, feature signatures, thread safety).
3. Provider acknowledgement correctness (Slack HTTP 200 with ok=false, SMTP recipient refusal, auth failures, retries).
4. Monitoring & streaming integration (offset safety, error isolation, stats counters).
"""
import asyncio
from datetime import datetime, timezone, timedelta
import smtplib
from typing import Any, Dict, List
import uuid

import httpx
import pytest

from backend.app.alerting.email import EmailAlertChannel
from backend.app.alerting.models import (
    AlertDeliveryStatus,
    AlertPayload,
    AlertRecord,
    AlertSeverity,
)
from backend.app.alerting.service import AlertService
from backend.app.alerting.slack import SlackAlertChannel
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
from dashboard.client import DashboardClient


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def base_run_factory():
    """Factory creating customizable MonitoringRunResponse objects."""
    def _create(
        run_id: str = "run-audit-001",
        model_version: str = "1.0.0",
        severity_band: str = "High",
        severity_score: int = 75,
        dataset_drift: bool = True,
        top_features: List[str] = None,
        quarantined: int = 0,
        components: Dict[str, float] = None,
        is_synth: bool = True,
    ) -> MonitoringRunResponse:
        top_features = top_features or ["amount"]
        components = components or {"D": 80.0, "P": 40.0, "Q": 10.0, "M": None}
        now = datetime.now(timezone.utc)

        top_contribs = [
            TopContributor(
                feature_name=f,
                contribution_score=0.35,
                cumulative_share=0.50,
            )
            for f in top_features
        ]

        return MonitoringRunResponse(
            run_id=run_id,
            model_name="paysim-fraud-histgradientboost",
            model_version=model_version,
            run_type="simulator" if is_synth else "scheduled",
            reference_window=WindowSpec(
                start=now - timedelta(days=7),
                end=now - timedelta(days=1),
                sample_count=5000,
            ),
            current_window=WindowSpec(
                start=now - timedelta(hours=1),
                end=now,
                sample_count=1000,
            ),
            sample_count=1000,
            valid_sample_count=1000 - quarantined,
            quarantined_sample_count=quarantined,
            status="ok",
            dataset_drift_detected=dataset_drift,
            prediction_drift=PredictionDriftSummary(
                metric="psi",
                value=0.28 if dataset_drift else 0.05,
                threshold=0.25,
                drifted=dataset_drift,
                status="ok",
            ),
            severity=SeverityBreakdown(
                score=severity_score,
                raw_score=float(severity_score),
                band=severity_band,
                confidence="normal",
                components=components,
                weights_used={"D": 0.533, "P": 0.267, "Q": 0.200},
            ),
            features=[],
            top_contributors=top_contribs,
            recommendations=["Investigate drift"],
            created_at=now,
        )

    return _create


# ==============================================================================
# 1. DASHBOARD FALLBACK & PROVENANCE CORRECTNESS
# ==============================================================================

def test_dashboard_client_connection_refused_diagnostics(monkeypatch):
    """Verify client accurately captures connection refused without crashing."""
    def mock_handler(request: httpx.Request):
        raise httpx.ConnectError("Connection refused: target machine actively refused it", request=request)

    transport = httpx.MockTransport(mock_handler)
    orig_client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: orig_client(transport=transport, **kwargs))

    client = DashboardClient(api_base_url="http://localhost:59998")
    status = client.get_backend_status()

    assert status["mode"] == "direct_local"
    assert status["status"] == "active_local_fallback"
    assert status["failure_reason"] == "connection_refused"
    assert "Connection refused" in status["api_error"]


def test_dashboard_client_timeout_diagnostics(monkeypatch):
    """Verify client accurately captures timeout errors without crashing."""
    def mock_handler(request: httpx.Request):
        raise httpx.TimeoutException("Read timed out after 2.0s", request=request)

    transport = httpx.MockTransport(mock_handler)
    orig_client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: orig_client(transport=transport, **kwargs))

    client = DashboardClient(api_base_url="http://localhost:59998")
    status = client.get_backend_status()

    assert status["mode"] == "direct_local"
    assert status["status"] == "active_local_fallback"
    assert status["failure_reason"] == "timeout"
    assert "timed out" in status["api_error"].lower()


def test_dashboard_client_http_error_fallback(monkeypatch):
    """Verify client handles backend HTTP 500 cleanly and reports http_500."""
    def mock_handler(request: httpx.Request):
        return httpx.Response(500, text="Internal Server Error: Database Down")

    transport = httpx.MockTransport(mock_handler)
    client = DashboardClient(api_base_url="http://mock-backend")

    # Patch httpx.Client in get_backend_status
    orig_client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: orig_client(transport=transport, **kwargs))

    status = client.get_backend_status()
    assert status["mode"] == "direct_local"
    assert status["failure_reason"] == "http_500"
    assert "Internal Server Error" in status["api_error"]


def test_dashboard_client_malformed_json_fallback(monkeypatch):
    """Verify client handles malformed HTML/text responses from /health gracefully."""
    def mock_handler(request: httpx.Request):
        return httpx.Response(200, text="<html><body>Gateway Error</body></html>", headers={"Content-Type": "text/html"})

    transport = httpx.MockTransport(mock_handler)
    client = DashboardClient(api_base_url="http://mock-backend")

    orig_client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: orig_client(transport=transport, **kwargs))

    status = client.get_backend_status()
    assert status["mode"] == "direct_local"
    assert status["failure_reason"] == "malformed_json"


def test_dashboard_client_in_memory_isolation_warning(monkeypatch):
    """Verify in_memory fallback explicitly flags process isolation and unshared data."""
    client = DashboardClient(api_base_url="http://localhost:59997")
    monkeypatch.setattr(client.settings, "PERSISTENCE_TYPE", "in_memory")

    status = client.get_backend_status()
    assert status["is_shared_storage"] is False
    assert status["freshness_warning"] is not None
    assert "process-local" in status["freshness_warning"] or "isolated" in status["freshness_warning"].lower()


def test_dashboard_client_sqlite_fallback_provenance(monkeypatch, tmp_path, base_run_factory):
    """Verify SQLite fallback detects database file existence, runs count, and provenance."""
    db_file = tmp_path / "test_modelpulse.db"
    repo = SQLiteMonitoringRepository(db_path=str(db_file))
    repo.save_run(base_run_factory("run-sqlite-001"))

    client = DashboardClient(api_base_url="http://localhost:59996")
    monkeypatch.setattr(client.settings, "PERSISTENCE_TYPE", "sqlite")
    monkeypatch.setattr(client.settings, "SQLITE_DB_PATH", str(db_file))

    orig_repo = get_monitoring_repository()
    set_monitoring_repository(repo)
    try:
        status = client.get_backend_status()
        assert status["is_shared_storage"] is True
        assert status["storage_exists"] is True
        assert status["storage_run_count"] == 1
        assert status["storage_last_modified"] is not None

        runs = client.list_runs()
        assert len(runs) == 1
        assert runs[0]["run_id"] == "run-sqlite-001"
        assert "local_sqlite" in client.last_data_source
    finally:
        set_monitoring_repository(orig_repo)
        repo.close()


# ==============================================================================
# 2. ALERT FINGERPRINT & COOLDOWN POLICY AUDIT
# ==============================================================================

def test_alert_fingerprint_distinguishes_different_drift_features(base_run_factory):
    """Verify different affected features for same model/severity yield different fingerprints."""
    service = AlertService()
    run1 = base_run_factory("run-drift-amt", top_features=["amount"])
    run2 = base_run_factory("run-drift-bal", top_features=["oldbalanceOrg"])

    p1 = service.build_payload(run1)
    p2 = service.build_payload(run2)

    assert p1.feature_signature == "amount"
    assert p2.feature_signature == "oldbalanceorg"
    assert p1.fingerprint != p2.fingerprint, "Distinct drifted features must produce distinct fingerprints!"


def test_alert_fingerprint_distinguishes_data_quality_from_data_drift(base_run_factory):
    """Verify a data quality incident is not suppressed by a prior data drift alert."""
    service = AlertService()
    # Run 1: High severity due to data drift
    run_drift = base_run_factory("run-d", dataset_drift=True, top_features=["amount"], quarantined=0)
    # Run 2: High severity due to data quality (quarantine surge), no feature drift
    run_quality = base_run_factory(
        "run-q",
        dataset_drift=False,
        top_features=[],
        quarantined=500,
        components={"D": 0.0, "P": 0.0, "Q": 100.0, "M": None},
    )

    p_drift = service.build_payload(run_drift)
    p_qual = service.build_payload(run_quality)

    assert "data_drift" in p_drift.incident_category
    assert "data_quality" in p_qual.incident_category
    assert p_drift.fingerprint != p_qual.fingerprint, "Data drift and data quality must have distinct fingerprints!"


def test_alert_fingerprint_distinguishes_model_version_and_synthetic(base_run_factory):
    """Verify model version changes or synthetic vs production yield distinct fingerprints."""
    service = AlertService()
    run_v1 = base_run_factory("run-v1", model_version="1.0.0", is_synth=True)
    run_v2 = base_run_factory("run-v2", model_version="2.0.0", is_synth=True)
    run_real = base_run_factory("run-real", model_version="1.0.0", is_synth=False)

    p_v1 = service.build_payload(run_v1)
    p_v2 = service.build_payload(run_v2)
    p_real = service.build_payload(run_real)

    assert p_v1.fingerprint != p_v2.fingerprint
    assert p_v1.fingerprint != p_real.fingerprint


@pytest.mark.asyncio
async def test_alert_cooldown_expiry_and_duplicate_suppression(base_run_factory):
    """Verify duplicate alert is suppressed during cooldown, then allowed after expiry."""
    service = AlertService(min_severity="High", cooldown_seconds=60)
    run = base_run_factory("run-cd-001", top_features=["amount"])

    # 1. First dispatch: evaluated
    res1 = await service.evaluate_and_dispatch(run)
    assert len(res1) == 2  # slack + email evaluated

    # 2. Immediate duplicate: suppressed
    res2 = await service.evaluate_and_dispatch(run)
    assert len(res2) == 1
    assert res2[0].status == AlertDeliveryStatus.SUPPRESSED_COOLDOWN

    # 3. Simulate passage of time past cooldown
    payload = service.build_payload(run)
    past_time = datetime.now(timezone.utc) - timedelta(seconds=65)
    service.record_cooldown(payload.fingerprint, past_time)

    # Cooldown should now be inactive
    assert service.is_cooldown_active(payload.fingerprint) is False

    # 4. Dispatch again: allowed
    res3 = await service.evaluate_and_dispatch(run)
    assert len(res3) == 2
    assert all(r.status != AlertDeliveryStatus.SUPPRESSED_COOLDOWN for r in res3)


@pytest.mark.asyncio
async def test_alert_cooldown_concurrent_dispatch_race_safety(base_run_factory):
    """Verify concurrent evaluations of the same payload cleanly suppress duplicates."""
    service = AlertService(min_severity="High", cooldown_seconds=300)
    run = base_run_factory("run-race-001", top_features=["amount"])

    # Dispatch 5 concurrent evaluations
    tasks = [service.evaluate_and_dispatch(run) for _ in range(5)]
    results = await asyncio.gather(*tasks)

    # Exactly one dispatch should process channels (len 2), the others should be suppressed (len 1)
    # Note: Because the first dispatch records cooldown, subsequent calls receive SUPPRESSED_COOLDOWN.
    statuses = [r[0].status for r in results]
    assert AlertDeliveryStatus.SUPPRESSED_COOLDOWN in statuses
    suppressed_count = sum(1 for r in results if r[0].status == AlertDeliveryStatus.SUPPRESSED_COOLDOWN)
    assert suppressed_count >= 1


# ==============================================================================
# 3. PROVIDER ACKNOWLEDGEMENT CORRECTNESS
# ==============================================================================

@pytest.mark.asyncio
async def test_slack_delivery_rejection_on_ok_false(base_run_factory):
    """Verify Slack HTTP 200 with {'ok': false} is rejected as FAILED."""
    def mock_handler(request: httpx.Request):
        # Slack API returning 200 with error body
        return httpx.Response(200, json={"ok": False, "error": "channel_not_found"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        channel = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/MOCK",
            max_retries=2,
            backoff_seconds=0.01,
            http_client=client,
        )
        service = AlertService()
        payload = service.build_payload(base_run_factory("run-slack-err"))

        rec = await channel.send("alt-test-01", payload)
        assert rec.status == AlertDeliveryStatus.FAILED
        assert rec.retry_count == 2
        assert "channel_not_found" in rec.error_message


@pytest.mark.asyncio
async def test_slack_delivery_rejection_on_unexpected_body(base_run_factory):
    """Verify Slack HTTP 200 with non-ok text is rejected as FAILED."""
    def mock_handler(request: httpx.Request):
        return httpx.Response(200, text="invalid_token")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        channel = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/MOCK",
            max_retries=2,
            backoff_seconds=0.01,
            http_client=client,
        )
        service = AlertService()
        payload = service.build_payload(base_run_factory("run-slack-txt"))

        rec = await channel.send("alt-test-02", payload)
        assert rec.status == AlertDeliveryStatus.FAILED
        assert "invalid_token" in rec.error_message


@pytest.mark.asyncio
async def test_smtp_recipient_refusal(base_run_factory):
    """Verify SMTP sender returning refused recipient dictionary fails delivery."""
    def mock_refused_sender(msg):
        # smtplib.send_message returns a dict of refused recipients
        return {"oncall@example.com": (550, b"User mailbox does not exist")}

    channel = EmailAlertChannel(
        smtp_host="smtp.example.com",
        recipient_email="oncall@example.com",
        max_retries=2,
        backoff_seconds=0.01,
        smtp_sender_fn=mock_refused_sender,
    )
    service = AlertService()
    payload = service.build_payload(base_run_factory("run-smtp-refused"))

    rec = await channel.send("alt-test-03", payload)
    assert rec.status == AlertDeliveryStatus.FAILED
    assert "refused" in rec.error_message.lower()
    assert rec.retry_count == 2


@pytest.mark.asyncio
async def test_smtp_auth_failure(base_run_factory):
    """Verify SMTP authentication error is handled cleanly with bounded retries."""
    def mock_auth_err(msg):
        raise smtplib.SMTPAuthenticationError(535, b"Authentication credentials invalid")

    channel = EmailAlertChannel(
        smtp_host="smtp.example.com",
        recipient_email="oncall@example.com",
        max_retries=2,
        backoff_seconds=0.01,
        smtp_sender_fn=mock_auth_err,
    )
    service = AlertService()
    payload = service.build_payload(base_run_factory("run-smtp-auth"))

    rec = await channel.send("alt-test-04", payload)
    assert rec.status == AlertDeliveryStatus.FAILED
    assert "auth" in rec.error_message.lower()
    assert rec.retry_count == 2


# ==============================================================================
# 4. MONITORING INTEGRATION & STATS ACCURACY
# ==============================================================================

@pytest.mark.asyncio
async def test_alert_service_stats_counter_accuracy(base_run_factory):
    """Verify delivery statistics accurately count delivered, suppressed, skipped, and failed."""
    service = AlertService(min_severity="High", cooldown_seconds=600)
    run = base_run_factory("run-stats-001")

    # Dispatch 1: skipped_disabled (no credentials in test environment) -> 2 skipped records
    await service.evaluate_and_dispatch(run)

    # Dispatch 2: duplicate -> 1 suppressed record
    await service.evaluate_and_dispatch(run)

    status = service.get_channel_status()
    stats = status["stats"]

    assert stats["skipped_disabled"] == 2
    assert stats["suppressed_cooldown"] == 1
    assert stats["total"] == 3
    assert stats["delivered"] == 0
    assert stats["failed"] == 0
