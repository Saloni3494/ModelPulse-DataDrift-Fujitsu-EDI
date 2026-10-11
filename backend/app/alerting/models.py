"""ModelPulse Alerting Contracts & Models (Phase 6).
Defines schemas for alert payloads, delivery records, channel status, and cooldown tracking.
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class AlertSeverity(str, Enum):
    LOW = "Low"
    MODERATE = "Moderate"
    HIGH = "High"
    CRITICAL = "Critical"

    @classmethod
    def rank(cls, severity: str) -> int:
        mapping = {
            cls.LOW.value.lower(): 1,
            cls.MODERATE.value.lower(): 2,
            cls.HIGH.value.lower(): 3,
            cls.CRITICAL.value.lower(): 4,
        }
        return mapping.get(severity.lower(), 1)


class AlertDeliveryStatus(str, Enum):
    DELIVERED = "delivered"
    FAILED = "failed"
    SUPPRESSED_COOLDOWN = "suppressed_cooldown"
    SKIPPED_DISABLED = "skipped_disabled"


class AlertPayload(BaseModel):
    """Normalized alert notification payload."""

    run_id: str
    model_name: str
    model_version: str
    severity_score: int
    severity_band: str
    dataset_drift_detected: bool
    top_drifted_features: List[str] = Field(default_factory=list)
    quarantined_count: int = 0
    total_samples: int = 0
    components: Dict[str, Optional[float]] = Field(default_factory=dict)
    dashboard_url: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_synthetic: bool = False

    @property
    def incident_category(self) -> str:
        """Classify primary incident category based on severity components and findings."""
        categories = []
        d_comp = self.components.get("data_drift") or self.components.get("D") or 0.0
        q_comp = self.components.get("data_quality") or self.components.get("Q") or 0.0
        p_comp = self.components.get("prediction_drift") or self.components.get("P") or 0.0
        m_comp = self.components.get("model_performance") or self.components.get("M") or 0.0

        if self.dataset_drift_detected or d_comp > 25.0:
            categories.append("data_drift")
        if self.quarantined_count > 0 or q_comp > 25.0:
            categories.append("data_quality")
        if p_comp > 25.0:
            categories.append("prediction_drift")
        if m_comp > 25.0:
            categories.append("model_degradation")

        if not categories:
            categories.append("general_severity")
        return "+".join(sorted(categories))

    @property
    def feature_signature(self) -> str:
        """Deterministic sorted signature of all affected/drifted features.
        Evaluates all reported drifted features to ensure different feature sets
        beyond the top 3 are never masked or erroneously merged.
        """
        if not self.top_drifted_features:
            return "none"
        clean = sorted({f.strip().lower() for f in self.top_drifted_features if f.strip()})
        return ",".join(clean) if clean else "none"

    @property
    def fingerprint(self) -> str:
        """Deterministic fingerprint for alert deduplication and cooldown.
        Combines model identity, version, severity band, incident category,
        affected feature signature, and synthetic flag to avoid suppressing materially
        distinct incidents while preventing alert storms for duplicate events.
        """
        synth_tag = "synthetic" if self.is_synthetic else "production"
        return (
            f"{self.model_name}:{self.model_version}:{self.severity_band}:"
            f"{self.incident_category}:{self.feature_signature}:{synth_tag}"
        )


class AlertRecord(BaseModel):
    """Audit record for alert dispatch attempts."""

    alert_id: str
    run_id: str
    channel: str  # 'slack', 'email'
    severity_band: str
    status: AlertDeliveryStatus
    recipient: Optional[str] = None
    sent_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: Optional[str] = None
    retry_count: int = 0


class AlertOutboxStatus(str, Enum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    DELIVERED = "delivered"
    SUPPRESSED_COOLDOWN = "suppressed_cooldown"
    SKIPPED_DISABLED = "skipped_disabled"
    FAILED = "failed"


class AlertOutboxRecord(BaseModel):
    """Durable transactional alert outbox record."""

    outbox_id: str  # Deterministic: f"outbox-{run_id}-{channel}"
    run_id: str
    channel: str    # 'slack', 'email'
    severity_band: str
    status: AlertOutboxStatus = AlertOutboxStatus.PENDING
    payload_json: str
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: Optional[str] = None
    claim_token: int = 0
    claimed_by: Optional[str] = None
