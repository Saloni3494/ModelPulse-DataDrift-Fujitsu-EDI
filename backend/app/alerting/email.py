"""ModelPulse Email Alert Channel (Phase 6).
Sends structured email notifications via SMTP with bounded retries and mockable transport.
"""
import asyncio
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import smtplib
from typing import Any, Callable, Dict, List, Optional

from backend.app.alerting.models import (
    AlertDeliveryStatus,
    AlertPayload,
    AlertRecord,
)

logger = logging.getLogger(__name__)


class EmailAlertChannel:
    """Delivers alerts via SMTP with delivery confirmation, retry safety, and mockable transport."""

    def __init__(
        self,
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        recipient_email: Optional[str] = None,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        smtp_sender_fn: Optional[Callable[[MIMEMultipart], None]] = None,
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port or 587
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.recipient_email = recipient_email
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self._smtp_sender_fn = smtp_sender_fn

    def is_configured(self) -> bool:
        """Return True if SMTP host and recipient are configured."""
        return bool(self.smtp_host and self.smtp_host.strip() and self.recipient_email and self.recipient_email.strip())

    def format_email(
        self, payload: AlertPayload, recipients: Optional[List[str]] = None
    ) -> MIMEMultipart:
        """Construct multipart email message."""
        subject = f"[ModelPulse Alert] {payload.severity_band} Severity ({payload.severity_score}/100) on {payload.model_name}"
        if payload.is_synthetic:
            subject += " [SYNTHETIC]"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.smtp_user or "alerts@modelpulse.internal"
        if recipients is not None:
            msg["To"] = ", ".join(recipients)
        else:
            msg["To"] = str(self.recipient_email)

        drift_text = "YES" if payload.dataset_drift_detected else "NO"
        drifted_feats = (
            ", ".join(payload.top_drifted_features)
            if payload.top_drifted_features
            else "None"
        )
        dashboard_url = payload.dashboard_url or "http://localhost:8501"

        text_body = f"""ModelPulse Alert Notification
=========================================
Severity: {payload.severity_band} (Score: {payload.severity_score}/100)
Model: {payload.model_name} (v{payload.model_version})
Run ID: {payload.run_id}
Timestamp: {payload.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}
Dataset Drift Detected: {drift_text}
Top Drifted Features: {drifted_feats}
Quarantined Records: {payload.quarantined_count}/{payload.total_samples}

View run details in the ModelPulse Dashboard:
{dashboard_url}
"""

        html_body = f"""
<html>
  <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px;">
      <h2 style="color: #1a202c; border-bottom: 2px solid #4a5568; padding-bottom: 8px;">
        ModelPulse Alert: <span style="color: {'#e53e3e' if payload.severity_band in ['High', 'Critical'] else '#d69e2e'};">{payload.severity_band} Severity</span>
      </h2>
      <table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
        <tr><td style="padding: 6px; font-weight: bold;">Severity Score:</td><td>{payload.severity_score}/100</td></tr>
        <tr><td style="padding: 6px; font-weight: bold;">Model:</td><td><code>{payload.model_name}:{payload.model_version}</code></td></tr>
        <tr><td style="padding: 6px; font-weight: bold;">Run ID:</td><td><code>{payload.run_id}</code></td></tr>
        <tr><td style="padding: 6px; font-weight: bold;">Drift Detected:</td><td><strong>{drift_text}</strong></td></tr>
        <tr><td style="padding: 6px; font-weight: bold;">Top Drifted Features:</td><td>{drifted_feats}</td></tr>
        <tr><td style="padding: 6px; font-weight: bold;">Quarantined Records:</td><td>{payload.quarantined_count}/{payload.total_samples}</td></tr>
      </table>
      <div style="margin-top: 20px; text-align: center;">
        <a href="{dashboard_url}" style="background-color: #3182ce; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block;">
          Open in Dashboard
        </a>
      </div>
      <p style="font-size: 11px; color: #718096; margin-top: 24px; text-align: center;">
        Sent by ModelPulse v2 MLOps Monitoring Platform
      </p>
    </div>
  </body>
</html>
"""
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
        return msg

    def _default_smtp_send(self, msg: MIMEMultipart) -> Optional[Dict[str, Any]]:
        """Live SMTP transmission."""
        with smtplib.SMTP(str(self.smtp_host), int(self.smtp_port), timeout=10.0) as server:
            server.ehlo()
            if server.has_extn("STARTTLS"):
                server.starttls()
                server.ehlo()
            if self.smtp_user and self.smtp_password:
                server.login(self.smtp_user, self.smtp_password)
            refused = server.send_message(msg)
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)
            return refused

    async def send(self, alert_id: str, payload: AlertPayload) -> AlertRecord:
        """Send formatted email with bounded retries and partial recipient retry safety."""
        if not self.is_configured():
            logger.info("[EmailAlert] SMTP host or recipient not configured. Skipping Email delivery.")
            return AlertRecord(
                alert_id=alert_id,
                run_id=payload.run_id,
                channel="email",
                severity_band=payload.severity_band,
                status=AlertDeliveryStatus.SKIPPED_DISABLED,
                recipient=None,
                error_message="SMTP host or recipient email not configured in environment.",
                retry_count=0,
            )

        all_recipients = [r.strip() for r in (self.recipient_email or "").split(",") if r.strip()]
        pending_recipients = list(all_recipients)
        send_fn = self._smtp_sender_fn or self._default_smtp_send

        attempt = 0
        last_error = None

        while attempt < self.max_retries and pending_recipients:
            attempt += 1
            msg = self.format_email(payload, recipients=pending_recipients)
            try:
                # Execute SMTP sender (synchronous wrapped in thread if needed)
                result = await asyncio.to_thread(send_fn, msg)
                if isinstance(result, dict) and result:
                    refused_addrs = set(result.keys())
                    pending_recipients = [r for r in pending_recipients if r in refused_addrs]
                    if not pending_recipients:
                        break
                    raise smtplib.SMTPRecipientsRefused(result)

                # All pending recipients delivered in this attempt
                pending_recipients = []
                break
            except smtplib.SMTPRecipientsRefused as exc:
                refused_info = exc.recipients if hasattr(exc, "recipients") else str(exc)
                refused_addrs = set(exc.recipients.keys()) if hasattr(exc, "recipients") and isinstance(exc.recipients, dict) else set()
                if refused_addrs:
                    pending_recipients = [r for r in pending_recipients if r in refused_addrs]
                last_error = f"SMTP recipients refused: {refused_info}"
                logger.warning(
                    f"[EmailAlert] Attempt {attempt}/{self.max_retries} recipient refusal (remaining {len(pending_recipients)}): {last_error}"
                )
            except smtplib.SMTPAuthenticationError as exc:
                last_error = f"SMTP authentication failed: {exc}"
                logger.warning(
                    f"[EmailAlert] Attempt {attempt}/{self.max_retries} auth failure: {last_error}"
                )
            except smtplib.SMTPConnectError as exc:
                last_error = f"SMTP connection failed: {exc}"
                logger.warning(
                    f"[EmailAlert] Attempt {attempt}/{self.max_retries} connection failure: {last_error}"
                )
            except Exception as exc:
                last_error = f"SMTP error ({type(exc).__name__}): {exc}"
                logger.warning(
                    f"[EmailAlert] Attempt {attempt}/{self.max_retries} failed: {last_error}"
                )

            if attempt < self.max_retries and pending_recipients:
                backoff = self.backoff_seconds * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)

        if not pending_recipients:
            logger.info(
                f"[EmailAlert] Successfully sent alert {alert_id} for run {payload.run_id} to {self.recipient_email}."
            )
            return AlertRecord(
                alert_id=alert_id,
                run_id=payload.run_id,
                channel="email",
                severity_band=payload.severity_band,
                status=AlertDeliveryStatus.DELIVERED,
                recipient=self.recipient_email,
                error_message=None,
                retry_count=attempt - 1,
            )

        # Retries exhausted
        logger.error(f"[EmailAlert] Email delivery failed after {attempt} attempts: {last_error}")
        return AlertRecord(
            alert_id=alert_id,
            run_id=payload.run_id,
            channel="email",
            severity_band=payload.severity_band,
            status=AlertDeliveryStatus.FAILED,
            recipient=self.recipient_email,
            error_message=last_error,
            retry_count=attempt,
        )
