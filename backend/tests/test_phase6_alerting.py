"""ModelPulse Phase 6 Alerting Subsystem Tests.
Verifies:
1. Threshold selection (alert triggered only when severity meets or exceeds threshold).
2. Anti-storm cooldown & fingerprint deduplication.
3. Bounded retries and delivery verification for Slack and Email channels.
4. Provider failure and missing credentials handling (graceful skip without exception).
5. Audit history tracking and API endpoint contracts.
"""
import asyncio
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
import pytest
import httpx

from backend.app.alerting.email import EmailAlertChannel
from backend.app.alerting.models import (
    AlertDeliveryStatus,
    AlertPayload,
    AlertRecord,
    AlertSeverity,
)
from backend.app.alerting.service import AlertService
from backend.app.alerting.slack import SlackAlertChannel
from backend.app.schemas.drift import FeatureDriftResult
from backend.app.schemas.monitoring import (
    MonitoringRunResponse,
    PredictionDriftSummary,
    SeverityBreakdown,
    WindowSpec,
)


@pytest.fixture
def sample_monitoring_run():
    """Create a high-severity monitoring run fixture."""
    now = datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)
    return MonitoringRunResponse(
        run_id="run-alert-fixture-001",
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        run_type="streaming_microbatch",
        reference_window=WindowSpec(start=now, end=now, sample_count=5000),
        current_window=WindowSpec(start=now, end=now + timedelta(minutes=5), sample_count=1000),
        sample_count=1000,
        valid_sample_count=980,
        quarantined_sample_count=20,
        status="ok",
        dataset_drift_detected=True,
        prediction_drift=PredictionDriftSummary(
            metric="psi", value=0.32, threshold=0.25, drifted=True, status="ok"
        ),
        severity=SeverityBreakdown(
            score=72,
            raw_score=71.8,
            band="High",
            confidence="normal",
            components={"D": 80.0, "P": 75.0, "Q": 40.0, "M": None},
            weights_used={"D": 0.5333, "P": 0.2667, "Q": 0.20},
        ),
        features=[],
        top_contributors=[],
        recommendations=["Investigate feature drift on 'amount'"],
        created_at=now,
    )


# ==============================================================================
# 1. THRESHOLD FILTERING & SEVERITY RANKING
# ==============================================================================

def test_alert_severity_ranking():
    """Verify severity band ranking hierarchy."""
    assert AlertSeverity.rank("Low") == 1
    assert AlertSeverity.rank("Moderate") == 2
    assert AlertSeverity.rank("High") == 3
    assert AlertSeverity.rank("Critical") == 4
    # Case insensitivity
    assert AlertSeverity.rank("critical") == 4
    assert AlertSeverity.rank("unknown") == 1


@pytest.mark.asyncio
async def test_alert_threshold_filtering(sample_monitoring_run):
    """Verify that runs below min_severity threshold do not trigger alerts."""
    # Min severity is Critical, but run is High -> should not trigger
    service = AlertService(min_severity="Critical", cooldown_seconds=60)
    records = await service.evaluate_and_dispatch(sample_monitoring_run)
    assert len(records) == 0

    # Min severity is High, run is High -> should trigger
    service_high = AlertService(min_severity="High", cooldown_seconds=60)
    records_high = await service_high.evaluate_and_dispatch(sample_monitoring_run)
    # Both Slack and Email evaluated
    assert len(records_high) == 2


# ==============================================================================
# 2. DEDUPLICATION & COOLDOWN ANTI-STORM PROTECTION
# ==============================================================================

@pytest.mark.asyncio
async def test_alert_cooldown_and_deduplication(sample_monitoring_run):
    """Verify subsequent alerts for the same model/severity are suppressed within cooldown."""
    service = AlertService(min_severity="High", cooldown_seconds=1800)

    # First dispatch: allowed
    res1 = await service.evaluate_and_dispatch(sample_monitoring_run)
    assert len(res1) == 2

    # Second immediate dispatch with identical run / fingerprint: suppressed by cooldown
    res2 = await service.evaluate_and_dispatch(sample_monitoring_run)
    assert len(res2) == 1
    assert res2[0].status == AlertDeliveryStatus.SUPPRESSED_COOLDOWN

    # Advance time beyond cooldown (1801 seconds later)
    future_time = datetime.now(timezone.utc) + timedelta(seconds=1801)
    payload = service.build_payload(sample_monitoring_run)
    assert service.is_cooldown_active(payload.fingerprint, now=future_time) is False


# ==============================================================================
# 3. SLACK ALERT CHANNEL: SUCCESS, FAILURE, AND RETRIES
# ==============================================================================

@pytest.mark.asyncio
async def test_slack_channel_success():
    """Verify Slack alert channel delivers with HTTP 200 confirmation."""
    def mock_handler(request: httpx.Request):
        assert "ModelPulse Alert" in request.content.decode("utf-8")
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        channel = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/TEST/TOKEN",
            http_client=client,
        )
        payload = AlertPayload(
            run_id="test-run-1",
            model_name="fraud-model",
            model_version="1.0.0",
            severity_score=85,
            severity_band="Critical",
            dataset_drift_detected=True,
        )
        record = await channel.send("alt-1", payload)

        assert record.status == AlertDeliveryStatus.DELIVERED
        assert record.channel == "slack"
        assert record.error_message is None
        assert record.retry_count == 0


@pytest.mark.asyncio
async def test_slack_channel_failure_and_retries():
    """Verify Slack channel retries on HTTP 500 and records FAILED status when exhausted."""
    call_count = 0

    def mock_failing_handler(request: httpx.Request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(mock_failing_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        channel = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/TEST/TOKEN",
            max_retries=3,
            backoff_seconds=0.01,
            http_client=client,
        )
        payload = AlertPayload(
            run_id="test-run-fail",
            model_name="fraud-model",
            model_version="1.0.0",
            severity_score=90,
            severity_band="Critical",
            dataset_drift_detected=True,
        )
        record = await channel.send("alt-fail", payload)

        assert record.status == AlertDeliveryStatus.FAILED
        assert call_count == 3
        assert "HTTP 500" in record.error_message


# ==============================================================================
# 4. EMAIL ALERT CHANNEL: SUCCESS, FAILURE, AND MOCKED TRANSPORT
# ==============================================================================

@pytest.mark.asyncio
async def test_email_channel_success():
    """Verify Email alert channel delivers and calls SMTP sender."""
    sent_messages = []

    def mock_smtp_sender(msg: MIMEMultipart):
        sent_messages.append(msg)

    channel = EmailAlertChannel(
        smtp_host="smtp.example.com",
        recipient_email="oncall@example.com",
        smtp_sender_fn=mock_smtp_sender,
    )
    payload = AlertPayload(
        run_id="test-run-email",
        model_name="fraud-model",
        model_version="1.0.0",
        severity_score=65,
        severity_band="High",
        dataset_drift_detected=True,
    )
    record = await channel.send("alt-email-1", payload)

    assert record.status == AlertDeliveryStatus.DELIVERED
    assert record.channel == "email"
    assert record.recipient == "oncall@example.com"
    assert len(sent_messages) == 1
    assert "High Severity" in sent_messages[0]["Subject"]


@pytest.mark.asyncio
async def test_email_channel_failure_and_retries():
    """Verify Email channel retries on SMTP exception and marks FAILED when exhausted."""
    attempts = 0

    def mock_failing_smtp(msg: MIMEMultipart):
        nonlocal attempts
        attempts += 1
        raise ConnectionRefusedError("SMTP connection refused")

    channel = EmailAlertChannel(
        smtp_host="smtp.example.com",
        recipient_email="oncall@example.com",
        max_retries=3,
        backoff_seconds=0.01,
        smtp_sender_fn=mock_failing_smtp,
    )
    payload = AlertPayload(
        run_id="test-run-email-fail",
        model_name="fraud-model",
        model_version="1.0.0",
        severity_score=75,
        severity_band="Critical",
        dataset_drift_detected=True,
    )
    record = await channel.send("alt-email-fail", payload)

    assert record.status == AlertDeliveryStatus.FAILED
    assert attempts == 3
    assert "SMTP error" in record.error_message


# ==============================================================================
# 5. MISSING CREDENTIALS & GRACEFUL DEGRADATION
# ==============================================================================

@pytest.mark.asyncio
async def test_missing_credentials_gracefully_skipped():
    """Verify unconfigured channels cleanly return SKIPPED_DISABLED without sending real traffic."""
    slack_channel = SlackAlertChannel(webhook_url=None)
    email_channel = EmailAlertChannel(smtp_host=None, recipient_email=None)

    payload = AlertPayload(
        run_id="test-unconfigured",
        model_name="fraud-model",
        model_version="1.0.0",
        severity_score=95,
        severity_band="Critical",
        dataset_drift_detected=True,
    )

    slack_rec = await slack_channel.send("a1", payload)
    assert slack_rec.status == AlertDeliveryStatus.SKIPPED_DISABLED

    email_rec = await email_channel.send("a2", payload)
    assert email_rec.status == AlertDeliveryStatus.SKIPPED_DISABLED


# ==============================================================================
# 6. ALERT HISTORY & API ENDPOINTS
# ==============================================================================

@pytest.mark.asyncio
async def test_alert_audit_history_and_status(sample_monitoring_run):
    """Verify AlertService maintains audit history accessible via get_history()."""
    service = AlertService(min_severity="High")
    await service.evaluate_and_dispatch(sample_monitoring_run)

    history = service.get_history(limit=10)
    assert len(history) >= 2
    assert any(h.channel == "slack" for h in history)
    assert any(h.channel == "email" for h in history)

    status = service.get_channel_status()
    assert "slack" in status
    assert "email" in status
    assert status["min_severity"] == "High"
