"""Phase 4.1 Regression & Correctness Audit Test Suite.
Validates:
1. Exact severity score calculation and resolution of the 64 vs 57 discrepancy.
2. Quarantine isolation: nullable prediction/score, no model calls on invalid records.
3. Distinguishability of probability 0.0 from unavailable (None).
4. All-invalid batch behavior: prediction drift status 'all_quarantined' with value None (not 0.0).
5. Mixed-batch sample count accounting (submitted, valid, quarantined) and denominator isolation.
6. Schema resilience: handling missing columns and reporting unexpected columns.
7. Persistence failure propagation without false durable-save claims.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd
import pytest

from backend.app.db.repository import InMemoryMonitoringRepository
from backend.app.schemas.monitoring import (
    MonitoringRunRequest,
    MonitoringRunResponse,
    PredictionDriftSummary,
    SeverityBreakdown,
)
from backend.app.schemas.predictions import (
    BatchPredictionRequest,
    BatchPredictionResponse,
    PredictionRequest,
    PredictionResponse,
)
from backend.app.services.model_service import ModelService
from backend.app.services.monitoring_service import MonitoringService
from backend.app.services.severity_service import (
    SeverityConfig,
    compute_severity_score,
)


@pytest.fixture
def valid_features() -> Dict[str, Any]:
    return {
        "step": 10,
        "type": "TRANSFER",
        "amount": 10000.0,
        "oldbalanceOrg": 50000.0,
        "newbalanceOrig": 40000.0,
        "oldbalanceDest": 0.0,
        "newbalanceDest": 10000.0,
    }


@pytest.fixture
def invalid_features_neg() -> Dict[str, Any]:
    return {
        "step": 10,
        "type": "TRANSFER",
        "amount": -500.0,  # Negative amount (invalid)
        "oldbalanceOrg": 50000.0,
        "newbalanceOrig": 40000.0,
        "oldbalanceDest": 0.0,
        "newbalanceDest": 10000.0,
    }


@pytest.fixture
def invalid_features_missing() -> Dict[str, Any]:
    return {
        "step": 10,
        "type": "PAYMENT",
        # "amount" missing
        "oldbalanceOrg": 1000.0,
        "newbalanceOrig": 500.0,
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
    }


# ==============================================================================
# 1. SEVERITY SCORE REGRESSION & FORMULA CONSISTENCY AUDIT
# ==============================================================================

def test_severity_score_discrepancy_reproduction_and_resolution():
    """Reproduce the Phase 4 report case (D=56.25, P=100.0, Q=0.0, M=None).
    Documented formula: (0.40*56.25 + 0.20*100.0 + 0.15*0.0) / 0.75 = 56.6667 -> 57.
    Asserts that the actual severity engine outputs 57 (not 64) and raw_score is 56.6667.
    """
    res = compute_severity_score(
        component_d=56.25,
        component_p=100.0,
        component_q=0.0,
        component_m=None,
        sample_count=500,
    )

    # 1. Effective weights verification
    # Base weights: D=0.40, P=0.20, Q=0.15, M=0.25 -> Active sum = 0.75
    # Eff D = 0.40 / 0.75 = 0.533333...
    # Eff P = 0.20 / 0.75 = 0.266667...
    # Eff Q = 0.15 / 0.75 = 0.200000...
    assert res.weights_used["D"] == pytest.approx(0.40 / 0.75, abs=1e-5)
    assert res.weights_used["P"] == pytest.approx(0.20 / 0.75, abs=1e-5)
    assert res.weights_used["Q"] == pytest.approx(0.15 / 0.75, abs=1e-5)
    assert res.weights_used["M"] == 0.0
    assert sum(res.weights_used.values()) == pytest.approx(1.0, abs=1e-5)

    # 2. Raw and rounded score assertion
    expected_raw = (0.40 * 56.25 + 0.20 * 100.0 + 0.15 * 0.0) / 0.75
    assert expected_raw == pytest.approx(56.6667, abs=1e-4)
    assert res.raw_score == pytest.approx(expected_raw, abs=1e-4)
    assert res.score == 57  # NOT 64
    assert res.score == round(res.raw_score)

    # 3. Band and confidence verification
    assert res.band == "High"  # >= 50 and < 75 is High
    assert res.confidence == "degraded"  # M missing -> degraded confidence


def test_severity_all_components_consistency_table():
    """Verify that component values, effective weights, raw score, rounded score,
    and band boundary mappings are strictly consistent across multi-component scenarios.
    """
    test_cases = [
        # (D, P, Q, M, expected_raw, expected_score, expected_band)
        (0.0, 0.0, 0.0, None, 0.0, 0, "Low"),
        (25.0, 25.0, 0.0, None, 20.0, 20, "Low"),  # (0.4*25 + 0.2*25)/0.75 = 15/0.75 = 20.0
        (50.0, 50.0, 50.0, 50.0, 50.0, 50, "High"),
        (75.0, 100.0, 0.0, 0.0, (0.4*75 + 0.2*100) / 1.0, 50, "High"),
        (100.0, 100.0, 100.0, 100.0, 100.0, 100, "Critical"),
    ]
    for d, p, q, m, exp_raw, exp_score, exp_band in test_cases:
        res = compute_severity_score(
            component_d=d, component_p=p, component_q=q, component_m=m, sample_count=500
        )
        assert res.raw_score == pytest.approx(exp_raw, abs=1e-3)
        assert res.score == exp_score
        assert res.band == exp_band
        assert round(res.raw_score) == res.score


# ==============================================================================
# 2. QUARANTINE ISOLATION & NULLABLE PREDICTION FIELDS
# ==============================================================================

def test_single_prediction_quarantined_has_none_prediction_and_score(invalid_features_neg):
    """Verify single prediction on quarantined record sets prediction=None, score=None,
    distinct from a valid classification.
    """
    svc = ModelService.get_instance()
    req = PredictionRequest(
        event_id="evt-inv-1",
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        features=invalid_features_neg,
    )
    resp = svc.predict_single(req)

    assert resp.validation_status == "quarantined"
    assert resp.prediction is None
    assert resp.score is None
    assert resp.error_reason is not None
    assert "non-negative" in resp.error_reason


def test_model_inference_never_invoked_on_invalid_records(invalid_features_neg, invalid_features_missing):
    """Verify the underlying scikit-learn pipeline is NEVER invoked when records fail validation."""
    svc = ModelService.get_instance()
    with patch.object(svc.pipeline, "predict_proba", wraps=svc.pipeline.predict_proba) as mock_predict:
        # 1. Single invalid request
        req = PredictionRequest(
            event_id="evt-inv-2",
            model_name="paysim-fraud-histgradientboost",
            model_version="1.0.0",
            features=invalid_features_neg,
        )
        svc.predict_single(req)
        assert mock_predict.call_count == 0

        # 2. Batch of all invalid requests
        batch_req = BatchPredictionRequest(
            model_name="paysim-fraud-histgradientboost",
            model_version="1.0.0",
            predictions=[
                req,
                PredictionRequest(
                    event_id="evt-inv-3",
                    model_name="paysim-fraud-histgradientboost",
                    model_version="1.0.0",
                    features=invalid_features_missing,
                ),
            ],
        )
        batch_resp = svc.predict_batch(batch_req)
        assert mock_predict.call_count == 0
        assert all(r.validation_status == "quarantined" for r in batch_resp.results)
        assert all(r.prediction is None for r in batch_resp.results)
        assert all(r.score is None for r in batch_resp.results)


def test_valid_zero_probability_distinguishable_from_none():
    """Ensure a valid predicted probability of 0.0 remains distinguishable from None."""
    svc = ModelService.get_instance()

    # Valid response with mock score 0.0
    valid_resp = PredictionResponse(
        event_id="evt-valid-0",
        model_name=svc.model_name,
        model_version=svc.model_version,
        prediction=0,
        score=0.0,
        validation_status="valid",
        error_reason=None,
        timestamp=datetime.now(timezone.utc),
    )

    # Quarantined response
    quarantined_resp = PredictionResponse(
        event_id="evt-quar",
        model_name=svc.model_name,
        model_version=svc.model_version,
        prediction=None,
        score=None,
        validation_status="quarantined",
        error_reason="Missing required feature",
        timestamp=datetime.now(timezone.utc),
    )

    assert valid_resp.score == 0.0
    assert valid_resp.score is not None
    assert quarantined_resp.score is None
    assert valid_resp.prediction == 0
    assert quarantined_resp.prediction is None


def test_batch_prediction_preserves_order_with_mixed_records(valid_features, invalid_features_neg):
    """Verify batch prediction maintains exact 1:1 index alignment for mixed valid/invalid inputs."""
    svc = ModelService.get_instance()

    features_valid_2 = dict(valid_features)
    features_valid_2["amount"] = 999.0

    items = [
        PredictionRequest(
            event_id="item-0-valid",
            model_name="paysim-fraud-histgradientboost",
            model_version="1.0.0",
            features=valid_features,
        ),
        PredictionRequest(
            event_id="item-1-invalid",
            model_name="paysim-fraud-histgradientboost",
            model_version="1.0.0",
            features=invalid_features_neg,
        ),
        PredictionRequest(
            event_id="item-2-valid",
            model_name="paysim-fraud-histgradientboost",
            model_version="1.0.0",
            features=features_valid_2,
        ),
    ]

    batch_req = BatchPredictionRequest(
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        predictions=items,
    )
    batch_resp = svc.predict_batch(batch_req)

    assert batch_resp.count == 3
    # Item 0: valid
    assert batch_resp.results[0].event_id == "item-0-valid"
    assert batch_resp.results[0].validation_status == "valid"
    assert batch_resp.results[0].score is not None

    # Item 1: quarantined
    assert batch_resp.results[1].event_id == "item-1-invalid"
    assert batch_resp.results[1].validation_status == "quarantined"
    assert batch_resp.results[1].score is None
    assert batch_resp.results[1].prediction is None

    # Item 2: valid
    assert batch_resp.results[2].event_id == "item-2-valid"
    assert batch_resp.results[2].validation_status == "valid"
    assert batch_resp.results[2].score is not None


# ==============================================================================
# 3. ALL-QUARANTINED BATCH MONITORING BEHAVIOR
# ==============================================================================

def test_all_quarantined_batch_monitoring_behavior():
    """Verify monitoring run where ALL records are quarantined:
    - Prediction drift returns status='all_quarantined' with value=None (NOT 0.0).
    - Component P is None.
    - Component Q is 100.0 (saturation due to 100% failure rate).
    - Status is 'quality_failure'.
    - Valid sample count is 0, quarantined sample count equals total.
    """
    repo = InMemoryMonitoringRepository()
    svc = MonitoringService(repository=repo)

    # Batch of 50 corrupted records (all negative amounts)
    corrupted_records = [
        {
            "step": i + 1,
            "type": "CASH_OUT",
            "amount": -100.0 * (i + 1),  # corrupt
            "oldbalanceOrg": 500.0,
            "newbalanceOrig": 0.0,
            "oldbalanceDest": 0.0,
            "newbalanceDest": 500.0,
        }
        for i in range(50)
    ]

    req = MonitoringRunRequest(
        current_data=corrupted_records,
        run_type="manual",
    )
    res = svc.execute_monitoring_run(req, persist=False)

    # 1. Sample accounting
    assert res.sample_count == 50
    assert res.valid_sample_count == 0
    assert res.quarantined_sample_count == 50

    # 2. Prediction drift MUST be unavailable/all_quarantined, NOT zero
    assert res.prediction_drift.status == "all_quarantined"
    assert res.prediction_drift.value is None
    assert res.prediction_drift.drifted is False

    # 3. Severity components: P omitted, Q maxed
    assert res.severity.components["P"] is None
    assert res.severity.components["Q"] == 100.0
    assert res.severity.weights_used["P"] == 0.0

    # 4. Overall run status
    assert res.status == "quality_failure"
    assert any("quarantined" in rec for rec in res.recommendations)


# ==============================================================================
# 4. MIXED BATCH DENOMINATORS & PREDICTION PSI INVARIANCE
# ==============================================================================

def test_quarantined_records_do_not_alter_prediction_psi():
    """Verify that adding quarantined records to a batch does NOT alter the prediction-score PSI.
    The prediction metrics must use valid_sample_count as the denominator.
    """
    repo = InMemoryMonitoringRepository()
    svc = MonitoringService(repository=repo)
    ref_df = svc.get_reference_data()

    # 1. Clean batch of 100 records
    clean_sample = ref_df.iloc[:100].to_dict(orient="records")

    req_clean = MonitoringRunRequest(
        current_data=clean_sample,
        run_type="manual",
    )
    res_clean = svc.execute_monitoring_run(req_clean, persist=False)

    # 2. Mixed batch: identical 100 clean records + 50 corrupted records
    corrupted_extra = [
        {
            "step": 1,
            "type": "PAYMENT",
            "amount": -999.0,  # invalid
            "oldbalanceOrg": 100.0,
            "newbalanceOrig": 0.0,
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
        }
        for _ in range(50)
    ]
    mixed_sample = list(clean_sample) + corrupted_extra

    req_mixed = MonitoringRunRequest(
        current_data=mixed_sample,
        run_type="manual",
    )
    res_mixed = svc.execute_monitoring_run(req_mixed, persist=False)

    # Check sample counts
    assert res_clean.sample_count == 100
    assert res_clean.valid_sample_count == 100
    assert res_clean.quarantined_sample_count == 0

    assert res_mixed.sample_count == 150
    assert res_mixed.valid_sample_count == 100
    assert res_mixed.quarantined_sample_count == 50

    # Prediction PSI must be identical (quarantined records excluded from inference)
    assert res_clean.prediction_drift.value is not None
    assert res_mixed.prediction_drift.value is not None
    assert res_clean.prediction_drift.value == pytest.approx(
        res_mixed.prediction_drift.value, abs=1e-5
    )
    assert res_clean.severity.components["P"] == pytest.approx(
        res_mixed.severity.components["P"], abs=1e-3
    )

    # But Component Q must reflect the quality failure in the mixed batch
    assert res_clean.severity.components["Q"] == 0.0
    assert res_mixed.severity.components["Q"] > 0.0


# ==============================================================================
# 5. DATA QUALITY FINDINGS & UNEXPECTED COLUMNS
# ==============================================================================

def test_unexpected_columns_logged_in_quality_findings():
    """Verify that unexpected extra columns are detected and logged in quality issues."""
    repo = InMemoryMonitoringRepository()
    svc = MonitoringService(repository=repo)
    ref_df = svc.get_reference_data()

    sample = ref_df.iloc[:50].to_dict(orient="records")
    # Add unexpected extra column to all records
    for row in sample:
        row["unrecognized_crypto_field"] = "BTC-12345"

    req = MonitoringRunRequest(
        current_data=sample,
        run_type="manual",
    )
    res = svc.execute_monitoring_run(req, persist=False)

    assert any("unrecognized_crypto_field" in rec for rec in res.recommendations)


def test_missing_required_column_quarantines_entire_batch():
    """Verify that when a required column is missing entirely from current data,
    all records are quarantined and prediction drift status is 'all_quarantined'.
    """
    repo = InMemoryMonitoringRepository()
    svc = MonitoringService(repository=repo)
    ref_df = svc.get_reference_data()

    # Drop required 'amount' column
    df_missing = ref_df.iloc[:40].copy()
    df_missing.drop(columns=["amount"], inplace=True)

    req = MonitoringRunRequest(
        current_data=df_missing.to_dict(orient="records"),
        run_type="manual",
    )
    res = svc.execute_monitoring_run(req, persist=False)

    assert res.sample_count == 40
    assert res.valid_sample_count == 0
    assert res.quarantined_sample_count == 40
    assert res.prediction_drift.status == "all_quarantined"
    assert res.prediction_drift.value is None
    assert res.severity.components["P"] is None
    assert res.severity.components["Q"] == 100.0


# ==============================================================================
# 6. PERSISTENCE FAILURE BEHAVIOR
# ==============================================================================

def test_persistence_failure_propagates_without_false_claim():
    """Verify that database/repository outages raise an exception rather than
    silently claiming a successful durable save.
    """
    repo = InMemoryMonitoringRepository()
    repo.simulate_failure = True  # Inject simulated DB failure

    svc = MonitoringService(repository=repo)
    req = MonitoringRunRequest(scenario_id="no_drift", scenario_samples=100)

    with pytest.raises(RuntimeError, match="Simulated database outage"):
        svc.execute_monitoring_run(req, persist=True)

    # Verify repository storage remains empty
    assert repo.count_runs() == 0
