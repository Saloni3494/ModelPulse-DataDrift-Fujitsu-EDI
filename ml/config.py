"""ModelPulse ML Configuration.
Defines dataset schemas, feature configurations, split thresholds, and model parameters.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class BaselineConfig:
    """Configuration for reproducible baseline fraud-detection model."""

    # Paths & Provenance
    data_path: str = "data/raw/paysim.csv"
    artifacts_dir: str = "ml/artifacts"
    dataset_name: str = "PaySim Mobile Money Fraud"
    dataset_source_url: str = "https://www.kaggle.com/datasets/ealaxi/paysim1"
    
    # Target and Temporal Fields
    target_column: str = "isFraud"
    temporal_column: str = "step"

    # Chronological Split Boundaries (PaySim step is in hours, total steps 1 to 744)
    # Train: steps 1..500 (~21 days, ~67%)
    # Val/Reference: steps 501..620 (~5 days, ~16%)
    # Held-out Test: steps 621..744 (~5 days, ~17%)
    train_max_step: int = 500
    val_max_step: int = 620

    # Leakage and Identifier Exclusions
    # isFraud: Target variable (must never be an input)
    # isFlaggedFraud: Rule-based heuristic flag (amount > 200,000); causes leakage/shortcut bias
    # nameOrig, nameDest: High-cardinality transaction IDs (overfits customer strings)
    excluded_columns: List[str] = field(
        default_factory=lambda: [
            "isFraud",
            "isFlaggedFraud",
            "nameOrig",
            "nameDest",
        ]
    )

    # Feature Schema Definitions
    raw_numeric_features: List[str] = field(
        default_factory=lambda: [
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
        ]
    )
    raw_categorical_features: List[str] = field(
        default_factory=lambda: [
            "type",
        ]
    )
    engineered_features: List[str] = field(
        default_factory=lambda: [
            "errorBalanceOrig",
            "errorBalanceDest",
            "hour_of_day",
            "day_of_week",
            "dest_type",
        ]
    )

    # Model Hyperparameters
    model_name: str = "paysim-fraud-histgradientboost"
    model_version: str = "1.0.0"
    random_seed: int = 42
    max_iter: int = 100
    learning_rate: float = 0.1
    max_leaf_nodes: int = 31
    min_samples_leaf: int = 20
    class_weight: str = "balanced"

    def get_artifacts_path(self) -> Path:
        """Return Path object for artifacts directory."""
        return Path(self.artifacts_dir)
