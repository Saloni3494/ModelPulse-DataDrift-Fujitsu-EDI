"""Drift result schemas proposed in Phase 0 audit.
These schemas define contracts for per-feature ensemble drift detection.
"""
from typing import List, Optional
from pydantic import BaseModel, Field


class TestResult(BaseModel):
    """Result of an individual statistical drift test."""

    __test__ = False  # Prevent pytest from collecting this schema as a test class
    metric: str = Field(..., description="Test name: psi, ks, wasserstein, chi2, js")
    value: float = Field(..., description="Calculated statistical score")
    threshold: float = Field(..., description="Decision threshold")
    p_value: Optional[float] = Field(
        default=None, description="p-value if applicable (KS, Chi2)"
    )
    exceeded: bool = Field(..., description="True if test exceeded decision threshold")
    reference_count: Optional[int] = Field(
        default=None, description="Number of valid reference samples"
    )
    current_count: Optional[int] = Field(
        default=None, description="Number of valid current samples"
    )
    status: str = Field(
        default="ok",
        description="Execution status: ok, insufficient_samples, constant_feature, invalid_input",
    )
    details: Optional[dict] = Field(
        default=None, description="Additional diagnostic details or warning flags"
    )


class FeatureDriftResult(BaseModel):
    """Per-feature multi-method drift result."""

    feature_name: str
    feature_type: str = Field(..., description="'numeric' or 'categorical'")
    tests: List[TestResult]
    votes: str = Field(..., description="Votes formatted as 'k/m', e.g., '2/3'")
    votes_count: int = Field(..., description="Number of tests indicating drift")
    total_tests: int = Field(..., description="Total tests evaluated")
    drift_detected: bool = Field(
        ..., description="True if votes_count >= k ensemble threshold"
    )
    drift_contribution: float = Field(
        ..., description="Normalized contribution: importance * normalized drift"
    )
    unseen_categories: Optional[List[str]] = None
    reference_mean: Optional[float] = None
    current_mean: Optional[float] = None
