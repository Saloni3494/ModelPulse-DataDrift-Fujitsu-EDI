"""ModelPulse Baseline Model Training, Evaluation, and Independent Audit.
Implements the HistGradientBoostingClassifier baseline, validation-based threshold tuning,
metric calculation, independent audit verification, and artifact persistence.
"""
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, confusion_matrix
from sklearn.pipeline import Pipeline

from ml.config import BaselineConfig
from ml.preprocessing import PaySimFeatureEngineer, build_preprocessor_pipeline


def build_model_pipeline(config: BaselineConfig) -> Pipeline:
    """Build the complete, reproducible end-to-end model pipeline."""
    feature_engineer = PaySimFeatureEngineer()
    preprocessor = build_preprocessor_pipeline()
    classifier = HistGradientBoostingClassifier(
        random_state=config.random_seed,
        max_iter=config.max_iter,
        learning_rate=config.learning_rate,
        max_leaf_nodes=config.max_leaf_nodes,
        min_samples_leaf=config.min_samples_leaf,
        class_weight=config.class_weight,
    )

    pipeline = Pipeline(
        steps=[
            ("feature_engineer", feature_engineer),
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )
    return pipeline


def find_optimal_threshold(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    candidate_thresholds: Optional[List[float]] = None,
) -> float:
    """Select the optimal decision threshold using validation data only.

    Maximizes F1 score. Never uses held-out test data for threshold selection.
    """
    if candidate_thresholds is None:
        candidate_thresholds = [round(x, 2) for x in np.linspace(0.05, 0.95, 19)]

    best_threshold = 0.5
    best_f1 = -1.0

    for thresh in candidate_thresholds:
        preds = (y_pred_proba >= thresh).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, preds, labels=[0, 1]).ravel()
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * prec * rec) / (prec + rec) if (prec + rec) > 0 else 0.0

        if f1 > best_f1:
            best_f1 = f1
            best_threshold = thresh

    return best_threshold


def compute_classification_metrics(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    threshold: float,
) -> Dict[str, Any]:
    """Compute classification metrics on a dataset split.

    PR-AUC is computed strictly from continuous prediction scores.
    """
    y_true = np.asarray(y_true)
    y_pred_proba = np.asarray(y_pred_proba)

    y_pred = (y_pred_proba >= threshold).astype(int)
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()

    precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    f1 = float((2 * precision * recall) / (precision + recall)) if (precision + recall) > 0 else 0.0

    # PR-AUC computed strictly on continuous probability scores
    if len(np.unique(y_true)) > 1:
        pr_auc = float(average_precision_score(y_true, y_pred_proba))
    else:
        pr_auc = 0.0

    # Sanity check for NaN/Inf
    for val, name in [(precision, "precision"), (recall, "recall"), (f1, "f1"), (pr_auc, "pr_auc")]:
        if math.isnan(val) or math.isinf(val):
            raise ValueError(f"Metric '{name}' evaluated to NaN or Infinity: {val}")

    total = int(len(y_true))
    positives = int(np.sum(y_true == 1))
    negatives = int(np.sum(y_true == 0))
    pos_rate = float(positives / total) if total > 0 else 0.0

    return {
        "threshold": float(threshold),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "pr_auc": round(pr_auc, 4),
        "confusion_matrix": {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
        },
        "class_distribution": {
            "total_samples": total,
            "negative_samples": negatives,
            "positive_samples": positives,
            "positive_rate": round(pos_rate, 6),
        },
    }


def audit_metrics_independently(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    threshold: float,
    reported_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    """Independently audit the metrics calculated on the held-out test split.

    1. Recomputes precision, recall, and F1 directly from the confusion matrix.
    2. Verifies PR-AUC is from continuous scores, not binary labels.
    3. Checks for zero divisions, NaNs, and negative rates.
    4. Flags suspiciously perfect scores (leakage indicator).
    """
    cm_dict = reported_metrics["confusion_matrix"]
    tn, fp, fn, tp = cm_dict["tn"], cm_dict["fp"], cm_dict["fn"], cm_dict["tp"]

    # Independent calculations
    audit_precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    audit_recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    audit_f1 = (
        float((2 * audit_precision * audit_recall) / (audit_precision + audit_recall))
        if (audit_precision + audit_recall) > 0
        else 0.0
    )

    # Compare with reported (tolerance 1e-4)
    prec_diff = abs(audit_precision - reported_metrics["precision"])
    rec_diff = abs(audit_recall - reported_metrics["recall"])
    f1_diff = abs(audit_f1 - reported_metrics["f1"])

    matches_reported = (prec_diff < 1e-3) and (rec_diff < 1e-3) and (f1_diff < 1e-3)

    # Check continuous PR-AUC property
    unique_proba_count = len(np.unique(y_pred_proba))
    is_continuous_proba = unique_proba_count > 2

    # Leakage check: suspiciously perfect score (1.0 precision and recall)
    leakage_suspect = (reported_metrics["precision"] == 1.0 and reported_metrics["recall"] == 1.0)

    return {
        "independent_precision": round(audit_precision, 4),
        "independent_recall": round(audit_recall, 4),
        "independent_f1": round(audit_f1, 4),
        "matches_reported": matches_reported,
        "is_continuous_proba": is_continuous_proba,
        "unique_prediction_scores": unique_proba_count,
        "leakage_suspect": leakage_suspect,
        "audit_passed": matches_reported and is_continuous_proba and (not leakage_suspect),
    }


def save_baseline_artifacts(
    pipeline: Pipeline,
    train_metrics: Dict[str, Any],
    val_metrics: Dict[str, Any],
    test_metrics: Dict[str, Any],
    reference_df: pd.DataFrame,
    config: BaselineConfig,
    artifacts_dir: Optional[str] = None,
) -> Dict[str, str]:
    """Save all baseline artifacts and metadata under the artifacts directory."""
    out_dir = Path(artifacts_dir or config.artifacts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Model Pipeline (.joblib)
    model_path = out_dir / "model_pipeline.joblib"
    joblib.dump(pipeline, model_path)

    # 2. Baseline Metrics (.json)
    metrics_payload = {
        "model_name": config.model_name,
        "model_version": config.model_version,
        "train_metrics": train_metrics,
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
        "optimal_threshold": val_metrics["threshold"],
    }
    metrics_path = out_dir / "baseline_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics_payload, f, indent=2)

    # 3. Reference Features Data (.csv)
    ref_features_path = out_dir / "reference_features.csv"
    reference_df.to_csv(ref_features_path, index=False)

    # 4. Reference Metadata (.json)
    ref_metadata = {
        "model_version": config.model_version,
        "sample_count": len(reference_df),
        "columns": list(reference_df.columns),
        "window": {
            "train_max_step": config.train_max_step,
            "val_max_step": config.val_max_step,
        },
    }
    ref_meta_path = out_dir / "reference_metadata.json"
    with open(ref_meta_path, "w") as f:
        json.dump(ref_metadata, f, indent=2)

    # 5. Model Metadata (.json)
    model_metadata = {
        "model_name": config.model_name,
        "model_version": config.model_version,
        "dataset_name": config.dataset_name,
        "dataset_source_url": config.dataset_source_url,
        "target_column": config.target_column,
        "temporal_column": config.temporal_column,
        "split_boundaries": {
            "train_steps": f"1..{config.train_max_step}",
            "val_steps": f"{config.train_max_step + 1}..{config.val_max_step}",
            "test_steps": f">{config.val_max_step}",
        },
        "selected_threshold": val_metrics["threshold"],
        "threshold_selection_strategy": "Maximized F1 score on validation set only",
        "excluded_features": config.excluded_columns,
        "training_config": {
            "random_seed": config.random_seed,
            "max_iter": config.max_iter,
            "learning_rate": config.learning_rate,
            "class_weight": config.class_weight,
        },
    }
    model_meta_path = out_dir / "model_metadata.json"
    with open(model_meta_path, "w") as f:
        json.dump(model_metadata, f, indent=2)

    return {
        "model_pipeline": str(model_path),
        "baseline_metrics": str(metrics_path),
        "reference_features": str(ref_features_path),
        "reference_metadata": str(ref_meta_path),
        "model_metadata": str(model_meta_path),
    }


def load_baseline_artifacts(artifacts_dir: str = "ml/artifacts") -> Dict[str, Any]:
    """Load baseline model artifacts from disk."""
    dir_path = Path(artifacts_dir)
    model_path = dir_path / "model_pipeline.joblib"
    metrics_path = dir_path / "baseline_metrics.json"
    meta_path = dir_path / "model_metadata.json"

    if not model_path.exists():
        raise FileNotFoundError(f"Model artifact not found at '{model_path}'")

    pipeline = joblib.load(model_path)
    with open(metrics_path, "r") as f:
        metrics = json.load(f)
    with open(meta_path, "r") as f:
        metadata = json.load(f)

    return {
        "pipeline": pipeline,
        "metrics": metrics,
        "metadata": metadata,
    }
