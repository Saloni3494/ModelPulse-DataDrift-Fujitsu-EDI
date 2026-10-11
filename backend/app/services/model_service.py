"""ModelPulse Model Loading and Inference Service.
Manages baseline fraud-detection model loading, thread-safe inference caching,
feature validation, and fraud probability scoring.
"""
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import joblib
import numpy as np
import pandas as pd

from backend.app.schemas.predictions import (
    PredictionRequest,
    PredictionResponse,
    BatchPredictionRequest,
    BatchPredictionResponse,
)
from ml.model import load_baseline_artifacts

REQUIRED_INPUT_COLUMNS = [
    "step",
    "type",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]

NUMERIC_INPUT_COLUMNS = [
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]


class ModelService:
    """Singleton service for model artifact management and prediction execution."""

    _instance: Optional["ModelService"] = None

    def __init__(self, artifacts_dir: str = "ml/artifacts"):
        self.artifacts_dir = Path(artifacts_dir)
        self.pipeline = None
        self.metrics = None
        self.metadata = None
        self.optimal_threshold: float = 0.05
        self.model_name: str = "paysim-fraud-histgradientboost"
        self.model_version: str = "1.0.0"
        self._loaded: bool = False

    @classmethod
    def get_instance(cls, artifacts_dir: str = "ml/artifacts") -> "ModelService":
        """Return cached singleton instance."""
        if cls._instance is None:
            cls._instance = cls(artifacts_dir=artifacts_dir)
            cls._instance.load_artifacts()
        return cls._instance

    def load_artifacts(self) -> None:
        """Load model pipeline and metadata from disk safely.

        Raises:
            FileNotFoundError: If artifact files are missing.
            RuntimeError: If artifact loading or unpickling fails.
        """
        model_path = self.artifacts_dir / "model_pipeline.joblib"
        metrics_path = self.artifacts_dir / "baseline_metrics.json"
        meta_path = self.artifacts_dir / "model_metadata.json"

        if not model_path.exists():
            raise FileNotFoundError(
                f"Model artifact not found at '{model_path}'. "
                "Baseline model must be trained in Phase 1 before running inference."
            )
        if not metrics_path.exists():
            raise FileNotFoundError(f"Baseline metrics not found at '{metrics_path}'.")
        if not meta_path.exists():
            raise FileNotFoundError(f"Model metadata not found at '{meta_path}'.")

        try:
            artifacts = load_baseline_artifacts(str(self.artifacts_dir))
            self.pipeline = artifacts["pipeline"]
            self.metrics = artifacts["metrics"]
            self.metadata = artifacts["metadata"]
            self.optimal_threshold = float(
                self.metrics.get("optimal_threshold", 0.05)
            )
            self.model_name = self.metadata.get("model_name", self.model_name)
            self.model_version = self.metadata.get(
                "model_version", self.model_version
            )
            self._loaded = True
        except Exception as exc:
            self._loaded = False
            raise RuntimeError(
                f"Failed to load or deserialize model artifacts from '{self.artifacts_dir}': {exc}"
            ) from exc

    def validate_features(
        self, features: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Validate input features against expected contract.

        Checks:
        1. Required columns presence
        2. Numeric type and finiteness
        3. Non-negativity constraints (amount, step, balances)
        4. Valid string type for transaction category

        Returns:
            Tuple of (is_valid, error_reason)
        """
        if not isinstance(features, dict):
            return False, "Features payload must be a key-value dictionary."

        # 1. Missing columns check
        missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in features]
        if missing:
            return False, f"Missing required feature columns: {missing}"

        # 2. Numeric columns validation
        for col in NUMERIC_INPUT_COLUMNS:
            val = features.get(col)
            if val is None:
                return False, f"Feature '{col}' cannot be null or None."
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                return False, f"Feature '{col}' must be numeric, got {type(val).__name__}."
            if math.isnan(val) or math.isinf(val):
                return False, f"Feature '{col}' must be finite (got NaN or Inf)."

        # 3. Value range constraints
        if features["amount"] < 0:
            return False, f"Feature 'amount' must be non-negative, got {features['amount']}."
        if features["step"] < 1:
            return False, f"Feature 'step' must be a positive integer (>= 1), got {features['step']}."
        if features["oldbalanceOrg"] < 0 or features["newbalanceOrig"] < 0:
            return False, "Origin balances must be non-negative."
        if features["oldbalanceDest"] < 0 or features["newbalanceDest"] < 0:
            return False, "Destination balances must be non-negative."

        # 4. Categorical type validation
        tx_type = features.get("type")
        if not isinstance(tx_type, str) or not tx_type.strip():
            return False, "Feature 'type' must be a non-empty string."

        return True, None

    def predict_single(self, request: PredictionRequest) -> PredictionResponse:
        """Execute inference on a single prediction request.

        If features fail validation, returns a quarantined response.
        """
        if not self._loaded or self.pipeline is None:
            self.load_artifacts()

        is_valid, error_reason = self.validate_features(request.features)

        if not is_valid:
            return PredictionResponse(
                event_id=request.event_id,
                model_name=self.model_name,
                model_version=self.model_version,
                prediction=None,
                score=None,
                validation_status="quarantined",
                error_reason=error_reason,
                timestamp=request.timestamp or datetime.now(timezone.utc),
            )

        # Build single-row DataFrame for pipeline transformation and scoring
        df = pd.DataFrame([request.features])
        proba = self.pipeline.predict_proba(df)
        fraud_score = float(proba[0, 1])
        prediction = 1 if fraud_score >= self.optimal_threshold else 0

        return PredictionResponse(
            event_id=request.event_id,
            model_name=self.model_name,
            model_version=self.model_version,
            prediction=prediction,
            score=round(fraud_score, 6),
            validation_status="valid",
            error_reason=None,
            timestamp=request.timestamp or datetime.now(timezone.utc),
        )

    def predict_batch(
        self, request: BatchPredictionRequest
    ) -> BatchPredictionResponse:
        """Execute batch inference, handling valid and quarantined records."""
        if not self._loaded or self.pipeline is None:
            self.load_artifacts()

        n = len(request.predictions)
        results: List[Optional[PredictionResponse]] = [None] * n
        valid_items: List[Tuple[int, PredictionRequest]] = []

        # First pass: validate each item
        for idx, item in enumerate(request.predictions):
            is_valid, error_reason = self.validate_features(item.features)
            if is_valid:
                valid_items.append((idx, item))
            else:
                results[idx] = PredictionResponse(
                    event_id=item.event_id,
                    model_name=self.model_name,
                    model_version=self.model_version,
                    prediction=None,
                    score=None,
                    validation_status="quarantined",
                    error_reason=error_reason,
                    timestamp=item.timestamp or datetime.now(timezone.utc),
                )

        # Batch inference on valid items for vectorization efficiency
        if valid_items:
            valid_df = pd.DataFrame([item.features for _, item in valid_items])
            probas = self.pipeline.predict_proba(valid_df)

            for (idx, item), proba in zip(valid_items, probas):
                fraud_score = float(proba[1])
                pred = 1 if fraud_score >= self.optimal_threshold else 0
                results[idx] = PredictionResponse(
                    event_id=item.event_id,
                    model_name=self.model_name,
                    model_version=self.model_version,
                    prediction=pred,
                    score=round(fraud_score, 6),
                    validation_status="valid",
                    error_reason=None,
                    timestamp=item.timestamp or datetime.now(timezone.utc),
                )

        return BatchPredictionResponse(
            model_name=self.model_name,
            model_version=self.model_version,
            count=len(results),
            results=[r for r in results if r is not None],
        )

    def get_info(self) -> Dict[str, Any]:
        """Return model metadata, operational status, and baseline threshold."""
        return {
            "loaded": self._loaded,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "optimal_threshold": self.optimal_threshold,
            "metrics": self.metrics,
            "metadata": self.metadata,
        }
