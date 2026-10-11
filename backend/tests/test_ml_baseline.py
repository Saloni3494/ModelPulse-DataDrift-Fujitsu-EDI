"""ModelPulse Baseline ML Automated Test Suite.
Validates chronological split ordering, zero target leakage, reproducible training,
feature schema contracts, invalid input handling, artifact persistence, and independent metric audit.
"""
import json
import numpy as np
import pandas as pd
import pytest
from pathlib import Path
import tempfile

from ml.config import BaselineConfig
from ml.dataset import generate_synthetic_paysim, split_chronological
from ml.preprocessing import (
    PaySimFeatureEngineer,
    build_preprocessor_pipeline,
    validate_raw_dataframe,
    LEAKAGE_COLUMNS,
)
from ml.model import (
    build_model_pipeline,
    find_optimal_threshold,
    compute_classification_metrics,
    audit_metrics_independently,
    save_baseline_artifacts,
    load_baseline_artifacts,
)


@pytest.fixture
def synthetic_df():
    """Provides reproducible synthetic PaySim test fixture."""
    return generate_synthetic_paysim(n_samples=2500, random_seed=42)


# 1. Chronological Split Ordering & Non-overlapping
def test_chronological_split_ordering(synthetic_df):
    """Verify chronological split enforces strict temporal ordering and non-overlapping windows."""
    train_df, val_df, test_df = split_chronological(
        synthetic_df, train_max_step=450, val_max_step=600, temporal_col="step"
    )

    # 1. Non-empty
    assert len(train_df) > 0
    assert len(val_df) > 0
    assert len(test_df) > 0
    assert len(train_df) + len(val_df) + len(test_df) == len(synthetic_df)

    # 2. Strict chronological boundaries
    assert train_df["step"].max() <= 450
    assert val_df["step"].min() > 450
    assert val_df["step"].max() <= 600
    assert test_df["step"].min() > 600

    # 3. Non-overlapping guarantees
    assert train_df["step"].max() < val_df["step"].min()
    assert val_df["step"].max() < test_df["step"].min()

    # 4. Inverted threshold must fail
    with pytest.raises(ValueError, match="strictly less"):
        split_chronological(synthetic_df, train_max_step=600, val_max_step=450)


# 2. Target Leakage & Feature Pipeline Isolation
def test_no_target_leakage(synthetic_df):
    """Verify isFraud and isFlaggedFraud are strictly stripped from model features."""
    engineer = PaySimFeatureEngineer()
    transformed = engineer.fit_transform(synthetic_df)

    # Neither target nor heuristic leak may exist in feature matrix
    for leak_col in LEAKAGE_COLUMNS:
        assert leak_col not in transformed.columns, f"Leakage column {leak_col} found in features!"

    # Customer string IDs must be dropped
    assert "nameOrig" not in transformed.columns
    assert "nameDest" not in transformed.columns

    # Derived dest_type and temporal features exist
    assert "dest_type" in transformed.columns
    assert "errorBalanceOrig" in transformed.columns
    assert "hour_of_day" in transformed.columns


def test_preprocessing_fit_isolation(synthetic_df):
    """Ensure preprocessor is fitted strictly on training data without inspecting test data."""
    train_df, _, test_df = split_chronological(synthetic_df, train_max_step=500, val_max_step=620)

    X_train = train_df.drop(columns=["isFraud"])
    X_test = test_df.drop(columns=["isFraud"])

    engineer = PaySimFeatureEngineer()
    preprocessor = build_preprocessor_pipeline()

    # Fit only on training data
    X_train_eng = engineer.fit_transform(X_train)
    preprocessor.fit(X_train_eng)

    # Transform test data using the fitted parameters
    X_test_eng = engineer.transform(X_test)
    X_test_proc = preprocessor.transform(X_test_eng)

    assert X_test_proc.shape[0] == len(test_df)
    assert not np.isnan(X_test_proc).any()


# 3. Reproducible Training & Prediction Output
def test_reproducible_training_output(synthetic_df):
    """Verify training with fixed random seed produces identical probability outputs."""
    train_df, _, test_df = split_chronological(synthetic_df, train_max_step=500, val_max_step=620)
    X_train = train_df.drop(columns=["isFraud"])
    y_train = train_df["isFraud"].values
    X_test = test_df.drop(columns=["isFraud"])

    config = BaselineConfig(random_seed=42, max_iter=30)

    pipeline_1 = build_model_pipeline(config)
    pipeline_1.fit(X_train, y_train)
    proba_1 = pipeline_1.predict_proba(X_test)

    pipeline_2 = build_model_pipeline(config)
    pipeline_2.fit(X_train, y_train)
    proba_2 = pipeline_2.predict_proba(X_test)

    assert np.allclose(proba_1, proba_2, atol=1e-7), "Model predictions differ across identical seed runs!"


# 4. Feature Schema, Prediction Dimensions, and Probability Ranges
def test_prediction_dimensions_and_probability_ranges(synthetic_df):
    """Verify output probabilities are strictly in [0.0, 1.0] and sum to 1.0 per row."""
    train_df, _, test_df = split_chronological(synthetic_df, train_max_step=500, val_max_step=620)
    X_train = train_df.drop(columns=["isFraud"])
    y_train = train_df["isFraud"].values
    X_test = test_df.drop(columns=["isFraud"])

    config = BaselineConfig(random_seed=42, max_iter=25)
    pipeline = build_model_pipeline(config)
    pipeline.fit(X_train, y_train)

    probas = pipeline.predict_proba(X_test)

    # Dimensions: (N, 2)
    assert probas.shape == (len(test_df), 2)
    # Bounded in [0.0, 1.0]
    assert np.all(probas >= 0.0)
    assert np.all(probas <= 1.0)
    # Sum to 1.0 per row
    row_sums = np.sum(probas, axis=1)
    assert np.allclose(row_sums, 1.0, atol=1e-5)


# 5. Invalid / Missing Input Handling
def test_invalid_input_validation():
    """Verify data validator raises explicit errors for corrupt/invalid inputs."""
    # Missing required column
    bad_df_missing = pd.DataFrame({
        "step": [1],
        "amount": [100.0],
        # type missing!
        "oldbalanceOrg": [500.0],
        "newbalanceOrig": [400.0],
        "oldbalanceDest": [0.0],
        "newbalanceDest": [100.0],
    })
    with pytest.raises(ValueError, match="Missing required"):
        validate_raw_dataframe(bad_df_missing)

    # Negative amount
    bad_df_negative = pd.DataFrame({
        "step": [1],
        "type": ["PAYMENT"],
        "amount": [-50.0],
        "oldbalanceOrg": [500.0],
        "newbalanceOrig": [450.0],
        "oldbalanceDest": [0.0],
        "newbalanceDest": [50.0],
    })
    with pytest.raises(ValueError, match="Negative transaction amounts"):
        validate_raw_dataframe(bad_df_negative)


def test_unseen_category_handling(synthetic_df):
    """Verify pipeline handles unexpected categorical values gracefully without crashing."""
    train_df, _, test_df = split_chronological(synthetic_df, train_max_step=500, val_max_step=620)
    X_train = train_df.drop(columns=["isFraud"])
    y_train = train_df["isFraud"].values

    pipeline = build_model_pipeline(BaselineConfig(random_seed=42, max_iter=20))
    pipeline.fit(X_train, y_train)

    # Inject unseen type category into test sample
    X_test_unseen = test_df.drop(columns=["isFraud"]).copy()
    X_test_unseen.loc[X_test_unseen.index[0], "type"] = "CRYPTO_CONVERSION_NEW"

    probas = pipeline.predict_proba(X_test_unseen)
    assert probas.shape[0] == len(X_test_unseen)
    assert not np.isnan(probas).any()


# 6. Artifact Saving, Loading & Metric JSON Validity
def test_artifact_saving_and_loading(synthetic_df):
    """Verify all artifacts save, reload, and produce identical predictions and valid metrics JSON."""
    train_df, val_df, test_df = split_chronological(synthetic_df, train_max_step=500, val_max_step=620)
    X_train = train_df.drop(columns=["isFraud"])
    y_train = train_df["isFraud"].values
    X_val = val_df.drop(columns=["isFraud"])
    y_val = val_df["isFraud"].values
    X_test = test_df.drop(columns=["isFraud"])
    y_test = test_df["isFraud"].values

    config = BaselineConfig(random_seed=42, max_iter=25)
    pipeline = build_model_pipeline(config)
    pipeline.fit(X_train, y_train)

    val_probas = pipeline.predict_proba(X_val)[:, 1]
    optimal_thresh = find_optimal_threshold(y_val, val_probas)

    val_metrics = compute_classification_metrics(y_val, val_probas, threshold=optimal_thresh)
    test_probas = pipeline.predict_proba(X_test)[:, 1]
    test_metrics = compute_classification_metrics(y_test, test_probas, threshold=optimal_thresh)

    with tempfile.TemporaryDirectory() as tmp_dir:
        saved_paths = save_baseline_artifacts(
            pipeline=pipeline,
            train_metrics=val_metrics,  # dummy for test
            val_metrics=val_metrics,
            test_metrics=test_metrics,
            reference_df=train_df,
            config=config,
            artifacts_dir=tmp_dir,
        )

        # Verify all 5 files were written
        for name, path_str in saved_paths.items():
            assert Path(path_str).exists(), f"Artifact {name} was not saved!"

        # Reload artifacts and verify predictions match
        loaded = load_baseline_artifacts(artifacts_dir=tmp_dir)
        reloaded_pipeline = loaded["pipeline"]
        reloaded_probas = reloaded_pipeline.predict_proba(X_test)[:, 1]
        assert np.allclose(test_probas, reloaded_probas, atol=1e-7)

        # Validate metrics JSON contents
        with open(saved_paths["baseline_metrics"], "r") as f:
            metrics_json = json.load(f)
        assert metrics_json["model_name"] == config.model_name
        assert "test_metrics" in metrics_json
        assert metrics_json["test_metrics"]["precision"] >= 0.0
        assert metrics_json["test_metrics"]["recall"] >= 0.0
        assert metrics_json["test_metrics"]["pr_auc"] >= 0.0


# 7. Independent Metric Audit Logic
def test_independent_metric_audit():
    """Verify independent audit recalculates metrics and detects leakage/anomalies."""
    # Synthetic test scenario with 100 samples
    y_true = np.array([0] * 90 + [1] * 10)
    # Continuous probabilities
    y_pred_proba = np.linspace(0.01, 0.99, 100)
    threshold = 0.50

    reported = compute_classification_metrics(y_true, y_pred_proba, threshold=threshold)
    audit = audit_metrics_independently(y_true, y_pred_proba, threshold, reported)

    assert audit["matches_reported"] is True
    assert audit["is_continuous_proba"] is True
    assert audit["leakage_suspect"] is False
    assert audit["audit_passed"] is True

    # Test that artificially perfect metrics flag leakage suspect
    perfect_reported = {
        "precision": 1.0,
        "recall": 1.0,
        "f1": 1.0,
        "pr_auc": 1.0,
        "confusion_matrix": {"tn": 90, "fp": 0, "fn": 0, "tp": 10},
    }
    audit_perfect = audit_metrics_independently(y_true, y_pred_proba, threshold, perfect_reported)
    assert audit_perfect["leakage_suspect"] is True
    assert audit_perfect["audit_passed"] is False
