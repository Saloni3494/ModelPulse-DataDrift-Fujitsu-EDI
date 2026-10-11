"""Monitoring run schemas proposed in Phase 0 audit.
These schemas define contracts for end-to-end monitoring runs and severity reporting.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from backend.app.schemas.drift import FeatureDriftResult


class WindowSpec(BaseModel):
    """Specification of a monitoring data window."""

    start: datetime
    end: datetime
    sample_count: int


class MonitoringRunRequest(BaseModel):
    """Request payload for executing an on-demand monitoring run."""

    model_name: str = "paysim-fraud-histgradientboost"
    model_version: str = "1.0.0"
    run_type: str = Field(
        default="manual",
        description="'manual', 'hourly', 'fast_path', 'simulator'",
    )
    scenario_id: Optional[str] = Field(
        default=None,
        description="Simulator scenario ID (e.g. 'no_drift', 'numeric_mean_variance_shift')",
    )
    scenario_seed: int = Field(default=42, description="Random seed for simulator")
    scenario_samples: int = Field(
        default=1000, description="Sample count for simulator scenario"
    )
    current_data: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Batch of current raw evaluation records"
    )
    ground_truth_labels: Optional[List[int]] = Field(
        default=None,
        description="Optional ground truth labels for current batch (when feedback available)",
    )
    run_id: Optional[str] = Field(
        default=None,
        description="Optional deterministic monitoring run ID for idempotent replay",
    )


class PredictionDriftSummary(BaseModel):
    """Prediction drift metric summary."""

    metric: str = "psi"
    value: Optional[float] = Field(
        default=None, description="Calculated statistical score (None if unavailable)"
    )
    threshold: float = 0.25
    drifted: bool = False
    status: str = Field(
        default="ok",
        description="'ok', 'unavailable', 'insufficient_samples', 'all_quarantined'",
    )


class SeverityBreakdown(BaseModel):
    """Deterministic 0-100 severity score and component breakdown."""

    score: int = Field(..., ge=0, le=100, description="Composite score 0-100")
    raw_score: Optional[float] = Field(
        default=None, description="Unrounded floating composite score"
    )
    band: str = Field(..., description="'Low', 'Moderate', 'High', 'Critical'")
    confidence: str = Field(..., description="'normal', 'low_sample', 'degraded'")
    components: Dict[str, Optional[float]] = Field(
        ..., description="Raw components: D (data), P (pred), Q (quality), M (metric)"
    )
    weights_used: Dict[str, float] = Field(
        ..., description="Renormalized component weights"
    )


class SeverityResult(BaseModel):
    """Comprehensive severity score evaluation report."""

    score: int = Field(..., ge=0, le=100, description="Bounded integer score 0-100")
    raw_score: float = Field(..., ge=0.0, le=100.0, description="Unrounded floating composite score")
    band: str = Field(..., description="'Low', 'Moderate', 'High', 'Critical'")
    confidence: str = Field(..., description="'normal', 'low_sample', 'degraded', 'unavailable'")
    components: Dict[str, Optional[float]] = Field(
        ..., description="Evaluated components: D, P, Q, M (in [0, 100] or None)"
    )
    weights_used: Dict[str, float] = Field(
        ..., description="Renormalized effective component weights"
    )
    original_weights: Dict[str, float] = Field(
        ..., description="Pre-renormalization configured weights"
    )
    thresholds: Dict[str, float] = Field(
        ..., description="Threshold configuration applied during evaluation"
    )
    evidence_counts: Dict[str, int] = Field(
        ..., description="Sample and test counts providing evidence"
    )
    status: str = Field(
        ..., description="'ok', 'no_evidence', 'quality_failure', 'degraded'"
    )
    explanation: str = Field(
        ..., description="Transparent breakdown explanation and rationale"
    )
    is_heuristic: bool = Field(
        default=True,
        description="Flag denoting explainable heuristic status, not a failure probability",
    )

    def to_breakdown(self) -> SeverityBreakdown:
        """Convert to basic SeverityBreakdown for monitoring summary."""
        return SeverityBreakdown(
            score=self.score,
            raw_score=self.raw_score,
            band=self.band,
            confidence=self.confidence,
            components=self.components,
            weights_used=self.weights_used,
        )


class TopContributor(BaseModel):
    """Leading feature contributor to dataset drift."""

    feature_name: str
    contribution_score: float
    cumulative_share: float


class MonitoringRunResponse(BaseModel):
    """Comprehensive monitoring run report."""

    run_id: str
    model_name: str
    model_version: str
    run_type: str = Field(..., description="'hourly', 'fast_path', 'manual'")
    reference_window: WindowSpec
    current_window: WindowSpec
    sample_count: int
    valid_sample_count: Optional[int] = Field(
        default=None, description="Count of clean, valid records evaluated by model"
    )
    quarantined_sample_count: Optional[int] = Field(
        default=None, description="Count of invalid/corrupt records quarantined"
    )
    status: str = Field(
        ..., description="'ok', 'insufficient_evidence', 'quality_failure'"
    )
    dataset_drift_detected: bool
    prediction_drift: PredictionDriftSummary
    severity: SeverityBreakdown
    features: List[FeatureDriftResult]
    top_contributors: List[TopContributor]
    recommendations: List[str]
    created_at: datetime
