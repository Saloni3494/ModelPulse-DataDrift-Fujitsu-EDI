"""ModelPulse Drift Simulator Ground-Truth Manifest.
Defines schemas and metadata structures for controlled drift scenarios.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


class ScenarioManifest(BaseModel):
    """Ground-truth manifest recording exact parameters of an injected drift scenario."""

    scenario_id: str = Field(
        ...,
        description="Unique identifier for scenario (e.g., 'numeric_mean_variance_shift')",
    )
    seed: int = Field(..., description="Random seed used for reproducible generation")
    changed_features: List[str] = Field(
        default_factory=list,
        description="List of feature names directly modified by this scenario",
    )
    drift_type: str = Field(
        ...,
        description="Type of injected shift: none, numeric_mean_variance, categorical_proportion, unseen_categories, sudden, gradual, data_quality",
    )
    magnitude: Union[float, Dict[str, Any]] = Field(
        ...,
        description="Shift magnitude multiplier, parameters dict, or corruption intensity",
    )
    start_window: Optional[Union[int, str]] = Field(
        default=None,
        description="Start step, timestamp, or sample index where drift begins",
    )
    end_window: Optional[Union[int, str]] = Field(
        default=None,
        description="End step, timestamp, or sample index where drift terminates",
    )
    sample_counts: Dict[str, int] = Field(
        ...,
        description="Reference and current sample counts: {'reference': N_ref, 'current': N_cur}",
    )
    injected_faults: List[str] = Field(
        default_factory=list,
        description="Explicit fault descriptions injected into the dataset",
    )
    class_distribution: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Target class counts and positive rate: {'total': N, 'fraud_count': k, 'fraud_rate': r}",
    )
    description: str = Field(
        ..., description="Human-readable summary of the scenario mechanics"
    )
    synthetic_notice: str = Field(
        default="SYNTHETIC FIXTURE - NOT REAL PAYSIM OBSERVATIONS",
        description="Explicit disclaimer distinguishing synthetic fixtures from real PaySim data",
    )
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="UTC generation timestamp",
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert manifest to Python dictionary."""
        return self.model_dump()

    def to_json(self, indent: int = 2) -> str:
        """Convert manifest to formatted JSON string."""
        return self.model_dump_json(indent=indent)
