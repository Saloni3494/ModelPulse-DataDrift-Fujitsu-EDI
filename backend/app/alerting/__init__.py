"""ModelPulse Alerting Subsystem (Phase 6).
Provides Slack and Email notifications, threshold filtering, and alert cooldown deduplication.
"""
from backend.app.alerting.models import (
    AlertDeliveryStatus,
    AlertOutboxRecord,
    AlertOutboxStatus,
    AlertPayload,
    AlertRecord,
    AlertSeverity,
)
from backend.app.alerting.service import AlertService, get_alert_service
from backend.app.alerting.slack import SlackAlertChannel
from backend.app.alerting.email import EmailAlertChannel

__all__ = [
    "AlertDeliveryStatus",
    "AlertOutboxRecord",
    "AlertOutboxStatus",
    "AlertPayload",
    "AlertRecord",
    "AlertSeverity",
    "AlertService",
    "get_alert_service",
    "SlackAlertChannel",
    "EmailAlertChannel",
]
