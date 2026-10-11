"""ModelPulse Alert Orchestrator Service (Phase 6).
Evaluates monitoring run severities, applies deduplication and cooldown policies,
and coordinates dispatch across Slack and Email channels.
"""
from datetime import datetime, timezone, timedelta
import logging
import threading
from typing import Any, Dict, List, Optional
import uuid

from backend.app.alerting.email import EmailAlertChannel
from backend.app.alerting.models import (
    AlertDeliveryStatus,
    AlertOutboxRecord,
    AlertOutboxStatus,
    AlertPayload,
    AlertRecord,
    AlertSeverity,
)
from backend.app.alerting.slack import SlackAlertChannel
from backend.app.core.config import get_settings
from backend.app.schemas.monitoring import MonitoringRunResponse

logger = logging.getLogger(__name__)


class AlertService:
    """Orchestrates alert threshold filtering, cooldown deduplication, and multi-channel dispatch."""

    def __init__(
        self,
        min_severity: str = "High",
        cooldown_seconds: int = 1800,  # 30-minute cooldown
        slack_channel: Optional[SlackAlertChannel] = None,
        email_channel: Optional[EmailAlertChannel] = None,
        dashboard_base_url: str = "http://localhost:8501",
    ):
        self.min_severity = min_severity
        self.cooldown_seconds = cooldown_seconds
        self.dashboard_base_url = dashboard_base_url

        settings = get_settings()
        self.slack_channel = slack_channel or SlackAlertChannel(
            webhook_url=settings.SLACK_WEBHOOK_URL
        )
        self.email_channel = email_channel or EmailAlertChannel(
            smtp_host=settings.SMTP_HOST,
            smtp_port=settings.SMTP_PORT,
            smtp_user=settings.SMTP_USER,
            smtp_password=settings.SMTP_PASSWORD,
            recipient_email=settings.ALERT_RECIPIENT_EMAIL,
        )

        self._lock = threading.Lock()
        # Process-local in-memory cooldown cache: fingerprint -> last_sent_at.
        # NOTE: Multi-worker deployments will maintain independent worker caches unless distributed storage (e.g. Redis) is configured.
        self._cooldown_cache: Dict[str, datetime] = {}
        # Audit history of dispatch records
        self._history: List[AlertRecord] = []
        self._max_history: int = 500
        # Aggregated delivery statistics
        self._stats: Dict[str, int] = {
            "delivered": 0,
            "suppressed_cooldown": 0,
            "skipped_disabled": 0,
            "failed": 0,
            "total": 0,
        }

    def is_cooldown_active(self, fingerprint: str, now: Optional[datetime] = None) -> bool:
        """Check if an alert for this fingerprint was sent within the cooldown window."""
        with self._lock:
            if fingerprint not in self._cooldown_cache:
                return False
            last_sent = self._cooldown_cache[fingerprint]
            current_time = now or datetime.now(timezone.utc)
            elapsed = (current_time - last_sent).total_seconds()
            return elapsed < self.cooldown_seconds

    def record_cooldown(self, fingerprint: str, sent_at: Optional[datetime] = None) -> None:
        """Update cooldown timestamp for a fingerprint."""
        with self._lock:
            self._cooldown_cache[fingerprint] = sent_at or datetime.now(timezone.utc)

    def clear_cooldown(self) -> None:
        """Clear cooldown cache (testing utility)."""
        with self._lock:
            self._cooldown_cache.clear()

    def build_payload(self, run: MonitoringRunResponse) -> AlertPayload:
        """Map MonitoringRunResponse to AlertPayload."""
        top_features = [tc.feature_name for tc in run.top_contributors]
        quarantined = run.quarantined_sample_count or 0
        is_synth = run.run_type == "simulator" or (
            hasattr(run, "scenario_id") and getattr(run, "scenario_id") is not None
        )

        dashboard_url = f"{self.dashboard_base_url}?run_id={run.run_id}"

        return AlertPayload(
            run_id=run.run_id,
            model_name=run.model_name,
            model_version=run.model_version,
            severity_score=run.severity.score,
            severity_band=run.severity.band,
            dataset_drift_detected=run.dataset_drift_detected,
            top_drifted_features=top_features,
            quarantined_count=quarantined,
            total_samples=run.sample_count,
            components=run.severity.components,
            dashboard_url=dashboard_url,
            created_at=run.created_at,
            is_synthetic=is_synth,
        )

    def build_outbox_records(self, run: MonitoringRunResponse) -> List[AlertOutboxRecord]:
        """Construct pending AlertOutboxRecord items without writing to repository."""
        run_rank = AlertSeverity.rank(run.severity.band)
        min_rank = AlertSeverity.rank(self.min_severity)
        if run_rank < min_rank:
            return []

        payload = self.build_payload(run)
        payload_json = payload.model_dump_json()
        now = datetime.now(timezone.utc)
        records: List[AlertOutboxRecord] = []
        for ch in ("slack", "email"):
            outbox_id = f"outbox-{run.run_id}-{ch}"
            records.append(
                AlertOutboxRecord(
                    outbox_id=outbox_id,
                    run_id=run.run_id,
                    channel=ch,
                    severity_band=run.severity.band,
                    status=AlertOutboxStatus.PENDING,
                    payload_json=payload_json,
                    retry_count=0,
                    created_at=now,
                    updated_at=now,
                )
            )
        return records

    def stage_outbox(self, run: MonitoringRunResponse) -> List[AlertOutboxRecord]:
        """Stage pending alert records into durable outbox prior to offset commit.
        Raises RuntimeError on persistence failure so callers can abort offset commits.
        """
        records = self.build_outbox_records(run)
        if not records:
            return []

        try:
            from backend.app.db.repository import get_monitoring_repository
            repo = get_monitoring_repository()
            for rec in records:
                repo.save_alert_outbox(rec)
            return records
        except Exception as exc:
            logger.error(f"[AlertService] Failed staging alert outbox for run {run.run_id}: {exc}")
            raise RuntimeError(f"Failed staging alert outbox for run {run.run_id}: {exc}") from exc

    async def evaluate_and_dispatch(
        self,
        run: MonitoringRunResponse,
        force: bool = False,
        stage_outbox_if_missing: bool = True,
    ) -> List[AlertRecord]:
        """Evaluate severity threshold, apply cooldown, and dispatch to Slack & Email."""
        run_rank = AlertSeverity.rank(run.severity.band)
        min_rank = AlertSeverity.rank(self.min_severity)

        # 1. Severity threshold check
        if not force and run_rank < min_rank:
            logger.debug(
                f"[AlertService] Run {run.run_id} severity '{run.severity.band}' is below threshold '{self.min_severity}'. No alert."
            )
            return []

        if stage_outbox_if_missing:
            self.stage_outbox(run)

        payload = self.build_payload(run)
        fingerprint = payload.fingerprint
        now = datetime.now(timezone.utc)
        results: List[AlertRecord] = []

        # 2. Cooldown deduplication check
        if not force and self.is_cooldown_active(fingerprint, now=now):
            logger.info(
                f"[AlertService] Alert for fingerprint '{fingerprint}' suppressed by {self.cooldown_seconds}s cooldown."
            )
            suppressed_record = AlertRecord(
                alert_id=f"alt-sup-{uuid.uuid4().hex[:8]}",
                run_id=run.run_id,
                channel="all",
                severity_band=run.severity.band,
                status=AlertDeliveryStatus.SUPPRESSED_COOLDOWN,
                error_message=f"Suppressed by cooldown ({self.cooldown_seconds}s window)",
                sent_at=now,
            )
            self._record_history(suppressed_record)
            try:
                repo = get_monitoring_repository()
                repo.update_alert_outbox_status(f"outbox-{run.run_id}-slack", "suppressed_cooldown")
                repo.update_alert_outbox_status(f"outbox-{run.run_id}-email", "suppressed_cooldown")
            except Exception:
                pass
            return [suppressed_record]

        # 3. Dispatch to Slack with deterministic alert ID
        slack_id = f"alt-slack-{run.run_id}"
        slack_record = await self.slack_channel.send(slack_id, payload)
        results.append(slack_record)
        self._record_history(slack_record)
        try:
            repo = get_monitoring_repository()
            repo.update_alert_outbox_status(
                f"outbox-{run.run_id}-slack",
                slack_record.status.value,
                slack_record.error_message,
                slack_record.retry_count,
            )
        except Exception:
            pass

        # 4. Dispatch to Email with deterministic alert ID
        email_id = f"alt-email-{run.run_id}"
        email_record = await self.email_channel.send(email_id, payload)
        results.append(email_record)
        self._record_history(email_record)
        try:
            repo = get_monitoring_repository()
            repo.update_alert_outbox_status(
                f"outbox-{run.run_id}-email",
                email_record.status.value,
                email_record.error_message,
                email_record.retry_count,
            )
        except Exception:
            pass

        # 5. Record cooldown for evaluated alert
        self.record_cooldown(fingerprint, now)

        return results

    async def recover_and_dispatch_pending(
        self, limit: int = 50, worker_id: str = "recovery-worker"
    ) -> List[AlertRecord]:
        """Recover and dispatch pending outbox alerts using atomic claim/lease to prevent duplicate dispatch."""
        try:
            from backend.app.db.repository import get_monitoring_repository
            repo = get_monitoring_repository()
            pending_records = repo.claim_pending_alert_outbox(limit=limit, worker_id=worker_id)
        except Exception as exc:
            logger.warning(f"[AlertService] Failed claiming pending outbox: {exc}")
            return []

        recovered: List[AlertRecord] = []
        now = datetime.now(timezone.utc)
        runs_dispatched_in_this_cycle = set()

        for outbox_rec in pending_records:
            try:
                payload = AlertPayload.model_validate_json(outbox_rec.payload_json)
                fingerprint = payload.fingerprint

                if payload.run_id not in runs_dispatched_in_this_cycle and self.is_cooldown_active(fingerprint, now=now):
                    repo.update_alert_outbox_status(
                        outbox_rec.outbox_id,
                        AlertOutboxStatus.SUPPRESSED_COOLDOWN,
                        claim_token=outbox_rec.claim_token,
                    )
                    continue

                if outbox_rec.channel == "slack":
                    alert_id = f"alt-slack-{payload.run_id}"
                    rec = await self.slack_channel.send(alert_id, payload)
                elif outbox_rec.channel == "email":
                    alert_id = f"alt-email-{payload.run_id}"
                    rec = await self.email_channel.send(alert_id, payload)
                else:
                    continue

                if rec.status == AlertDeliveryStatus.DELIVERED:
                    updated = repo.update_alert_outbox_status(
                        outbox_rec.outbox_id,
                        AlertOutboxStatus.DELIVERED,
                        rec.error_message,
                        rec.retry_count,
                        claim_token=outbox_rec.claim_token,
                    )
                    if updated:
                        self.record_cooldown(fingerprint, now)
                    else:
                        logger.warning(
                            f"[AlertService] Fencing token rejection for {outbox_rec.outbox_id} (token={outbox_rec.claim_token}). "
                            "Lease expired and record was reclaimed by another worker."
                        )
                elif rec.status == AlertDeliveryStatus.SKIPPED_DISABLED:
                    repo.update_alert_outbox_status(
                        outbox_rec.outbox_id,
                        AlertOutboxStatus.SKIPPED_DISABLED,
                        rec.error_message,
                        rec.retry_count,
                        claim_token=outbox_rec.claim_token,
                    )
                else:
                    repo.update_alert_outbox_status(
                        outbox_rec.outbox_id,
                        AlertOutboxStatus.FAILED,
                        rec.error_message,
                        max(rec.retry_count, outbox_rec.retry_count + 1),
                        claim_token=outbox_rec.claim_token,
                    )

                self._record_history(rec)
                recovered.append(rec)
                runs_dispatched_in_this_cycle.add(payload.run_id)
            except Exception as exc:
                logger.error(f"[AlertService] Error recovering outbox entry {outbox_rec.outbox_id}: {exc}")
                final_retries = outbox_rec.retry_count + 1
                status = (
                    AlertOutboxStatus.FAILED
                    if final_retries >= outbox_rec.max_retries
                    else AlertOutboxStatus.PENDING
                )
                repo.update_alert_outbox_status(
                    outbox_rec.outbox_id,
                    status,
                    error_message=f"Recovery dispatch error: {exc}",
                    retry_count=final_retries,
                    claim_token=outbox_rec.claim_token,
                )

        return recovered

    def _record_history(self, record: AlertRecord) -> None:
        """Append to in-memory audit history and update aggregated stats."""
        with self._lock:
            self._history.insert(0, record)
            if len(self._history) > self._max_history:
                self._history.pop()

            self._stats["total"] += 1
            stat_key = record.status.value
            self._stats[stat_key] = self._stats.get(stat_key, 0) + 1

    def get_history(self, limit: int = 50) -> List[AlertRecord]:
        """Return most recent alert records."""
        with self._lock:
            return list(self._history[:limit])

    def get_channel_status(self) -> Dict[str, Any]:
        """Return configuration, stats, and health status of alert channels."""
        with self._lock:
            cached_fingerprints = len(self._cooldown_cache)
            stats_snapshot = dict(self._stats)

        return {
            "min_severity": self.min_severity,
            "cooldown_seconds": self.cooldown_seconds,
            "cached_fingerprints_count": cached_fingerprints,
            "stats": stats_snapshot,
            "slack": {
                "configured": self.slack_channel.is_configured(),
                "webhook_masked": (
                    f"...{self.slack_channel.webhook_url[-12:]}"
                    if self.slack_channel.is_configured()
                    else None
                ),
            },
            "email": {
                "configured": self.email_channel.is_configured(),
                "smtp_host": self.email_channel.smtp_host if self.email_channel.is_configured() else None,
                "recipient": self.email_channel.recipient_email if self.email_channel.is_configured() else None,
            },
        }


# Singleton instance
_alert_service_instance: Optional[AlertService] = None
_service_lock = threading.Lock()


def get_alert_service() -> AlertService:
    """Return AlertService singleton."""
    global _alert_service_instance
    with _service_lock:
        if _alert_service_instance is None:
            _alert_service_instance = AlertService()
        return _alert_service_instance
