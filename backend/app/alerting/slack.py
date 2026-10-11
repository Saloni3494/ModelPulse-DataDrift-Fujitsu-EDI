"""ModelPulse Slack Alert Channel (Phase 6).
Sends structured webhook notifications to Slack with bounded retries and delivery verification.
"""
import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, Optional
import httpx

from backend.app.alerting.models import (
    AlertDeliveryStatus,
    AlertPayload,
    AlertRecord,
)

logger = logging.getLogger(__name__)


class SlackAlertChannel:
    """Delivers alerts to Slack incoming webhook with delivery verification and retry safety."""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        self.webhook_url = webhook_url
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self._http_client = http_client

    def is_configured(self) -> bool:
        """Return True if Slack webhook URL is configured."""
        return bool(self.webhook_url and self.webhook_url.strip())

    def format_message(self, payload: AlertPayload) -> Dict[str, Any]:
        """Build structured Slack message."""
        severity_emojis = {
            "Low": "🟢",
            "Moderate": "🟡",
            "High": "🟠",
            "Critical": "🔴",
        }
        emoji = severity_emojis.get(payload.severity_band, "⚠️")

        drift_text = "YES (Drift Detected)" if payload.dataset_drift_detected else "NO"
        drifted_feats = (
            ", ".join(payload.top_drifted_features)
            if payload.top_drifted_features
            else "None"
        )
        dashboard_ref = (
            f"<{payload.dashboard_url}|Open Run in Dashboard>"
            if payload.dashboard_url
            else "N/A"
        )

        header = f"{emoji} *ModelPulse Alert: {payload.severity_band.upper()} Severity ({payload.severity_score}/100)*"
        if payload.is_synthetic:
            header += " `[SYNTHETIC SCENARIO]`"

        fields = [
            {"type": "mrkdwn", "text": f"*Model:* `{payload.model_name}:{payload.model_version}`"},
            {"type": "mrkdwn", "text": f"*Run ID:* `{payload.run_id}`"},
            {"type": "mrkdwn", "text": f"*Drift Detected:* {drift_text}"},
            {"type": "mrkdwn", "text": f"*Top Drifted Features:* {drifted_feats}"},
            {"type": "mrkdwn", "text": f"*Quarantined Records:* {payload.quarantined_count}/{payload.total_samples}"},
            {"type": "mrkdwn", "text": f"*Dashboard Link:* {dashboard_ref}"},
        ]

        return {
            "text": f"ModelPulse Alert: {payload.severity_band} severity on {payload.model_name}",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": header},
                },
                {
                    "type": "section",
                    "fields": fields,
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Timestamp: {payload.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')} | ModelPulse MLOps Engine",
                        }
                    ],
                },
            ],
        }

    async def send(self, alert_id: str, payload: AlertPayload) -> AlertRecord:
        """Send formatted Slack message with bounded retries.
        Guarantees: Does NOT report alert as delivered until provider confirms HTTP 200.
        """
        if not self.is_configured():
            logger.info("[SlackAlert] Webhook URL not configured. Skipping Slack delivery.")
            return AlertRecord(
                alert_id=alert_id,
                run_id=payload.run_id,
                channel="slack",
                severity_band=payload.severity_band,
                status=AlertDeliveryStatus.SKIPPED_DISABLED,
                recipient=None,
                error_message="Slack webhook URL not configured in environment.",
                retry_count=0,
            )

        message_body = self.format_message(payload)
        client = self._http_client or httpx.AsyncClient(timeout=10.0)
        should_close_client = self._http_client is None

        attempt = 0
        last_error = None

        try:
            while attempt < self.max_retries:
                attempt += 1
                try:
                    resp = await client.post(
                        str(self.webhook_url),
                        json=message_body,
                        headers={"Content-Type": "application/json"},
                    )
                    if resp.status_code == 200:
                        is_confirmed = True
                        err_reason = None
                        try:
                            json_data = resp.json()
                            if isinstance(json_data, dict) and json_data.get("ok") is False:
                                is_confirmed = False
                                err_reason = f"Slack returned ok=false ({json_data.get('error', 'unknown error')})"
                        except Exception:
                            # Slack incoming webhooks return plain text 'ok' on success
                            raw_text = resp.text.strip()
                            if raw_text.lower() not in ("ok", ""):
                                is_confirmed = False
                                err_reason = f"Slack returned unexpected text: '{raw_text}'"

                        if is_confirmed:
                            logger.info(
                                f"[SlackAlert] Successfully delivered alert {alert_id} for run {payload.run_id} to Slack."
                            )
                            return AlertRecord(
                                alert_id=alert_id,
                                run_id=payload.run_id,
                                channel="slack",
                                severity_band=payload.severity_band,
                                status=AlertDeliveryStatus.DELIVERED,
                                recipient="slack-channel",
                                error_message=None,
                                retry_count=attempt - 1,
                            )
                        else:
                            last_error = f"Slack delivery rejected: {err_reason}"
                            logger.warning(
                                f"[SlackAlert] Attempt {attempt}/{self.max_retries} rejected: {last_error}"
                            )
                    else:
                        last_error = f"Slack returned HTTP {resp.status_code}: {resp.text}"
                        logger.warning(
                            f"[SlackAlert] Attempt {attempt}/{self.max_retries} failed: {last_error}"
                        )
                except Exception as exc:
                    last_error = f"HTTP transport error: {exc}"
                    logger.warning(
                        f"[SlackAlert] Attempt {attempt}/{self.max_retries} connection error: {last_error}"
                    )

                if attempt < self.max_retries:
                    backoff = self.backoff_seconds * (2 ** (attempt - 1))
                    await asyncio.sleep(backoff)

            # All retries exhausted
            logger.error(f"[SlackAlert] Delivery failed after {attempt} attempts: {last_error}")
            return AlertRecord(
                alert_id=alert_id,
                run_id=payload.run_id,
                channel="slack",
                severity_band=payload.severity_band,
                status=AlertDeliveryStatus.FAILED,
                recipient="slack-channel",
                error_message=last_error,
                retry_count=attempt,
            )
        finally:
            if should_close_client:
                await client.aclose()
