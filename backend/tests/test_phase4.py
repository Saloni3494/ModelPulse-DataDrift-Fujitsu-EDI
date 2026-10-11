"""Phase 4 Test Suite: Inference API, Monitoring Orchestration, and Persistence Foundation.
Verifies:
1. Valid single and batch predictions via ModelService and FastAPI endpoints.
2. Feature validation, missing features, non-finite values, and unseen categories.
3. Missing artifacts error handling without silent fabrication.
4. Reference dataset immutability during monitoring execution.
5. No-drift and controlled-drift runs with synthetic fixtures.
6. Schema mismatch and quality failures handling without pipeline crashes.
7. Prediction drift calculations and independent verification.
8. Delayed labels handling (Component M is None, dynamic weight renormalisation).
9. Ground truth label evaluation when feedback is available.
10. Persistence success, retrieval, pagination, duplicate ID handling, and outage behavior.
11. SQLite repository persistence and ACID transaction behavior.
12. HTTP status codes, error handling, and reproducibility across repeated runs.
"""
from datetime import datetime, timezone
import math
import uuid
import numpy as np
import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient

from backend.app.main import app
from backend.app.schemas.predictions import (
    PredictionRequest,
    PredictionResponse,
    BatchPredictionRequest,
    BatchPredictionResponse,
)
from backend.app.schemas.monitoring import (
    MonitoringRunRequest,
    MonitoringRunResponse,
)
from backend.app.services.model_service import ModelService
from backend.app.services.monitoring_service import MonitoringService
from backend.app.db.repository import (
    InMemoryMonitoringRepository,
    SQLiteMonitoringRepository,
    get_monitoring_repository,
    set_monitoring_repository,
)


@pytest.fixture
def valid_features():
    """Valid transaction features fixture matching baseline schema."""
    return {
        "step": 100,
        "type": "TRANSFER",
        "amount": 25000.0,
        "oldbalanceOrg": 25000.0,
        "newbalanceOrig": 0.0,
        "oldbalanceDest": 5000.0,
        "newbalanceDest": 30000.0,
        "nameDest": "C123456789",
    }


@pytest.fixture
def test_repo():
    """Fresh in-memory repository fixture for isolated test execution."""
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)
    return repo


# ──────────────────────────────────────────────────────────────────────────────
# 1. MODEL SERVICE & INFERENCE TESTS
# ──────────────────────────────────────────────────────────────────────────────

def test_model_service_loads_cleanly():
    """Verify ModelService loads artifacts, threshold, and metadata from disk."""
    service = ModelService.get_instance()
    assert service._loaded is True
    assert service.pipeline is not None
    assert service.optimal_threshold == 0.05
    assert service.model_name == "paysim-fraud-histgradientboost"
    assert service.model_version == "1.0.0"


def test_model_service_predict_single_valid(valid_features):
    """Verify single prediction produces valid classification and bounded score."""
    service = ModelService.get_instance()
    req = PredictionRequest(
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        features=valid_features,
    )
    resp = service.predict_single(req)

    assert isinstance(resp, PredictionResponse)
    assert resp.event_id == req.event_id
    assert resp.validation_status == "valid"
    assert resp.error_reason is None
    assert resp.prediction in (0, 1)
    assert 0.0 <= resp.score <= 1.0


def test_model_service_predict_batch_valid(valid_features):
    """Verify batch prediction processes multiple valid items with correct mapping."""
    service = ModelService.get_instance()
    req1 = PredictionRequest(
        event_id="evt-1",
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        features=valid_features,
    )
    features2 = dict(valid_features)
    features2["amount"] = 500.0
    req2 = PredictionRequest(
        event_id="evt-2",
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        features=features2,
    )

    batch_req = BatchPredictionRequest(
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        predictions=[req1, req2],
    )
    batch_resp = service.predict_batch(batch_req)

    assert isinstance(batch_resp, BatchPredictionResponse)
    assert batch_resp.count == 2
    assert len(batch_resp.results) == 2
    assert batch_resp.results[0].event_id == "evt-1"
    assert batch_resp.results[1].event_id == "evt-2"
    assert all(r.validation_status == "valid" for r in batch_resp.results)


def test_model_service_invalid_features_quarantined(valid_features):
    """Verify invalid payloads (missing features, non-finite values, negatives) are quarantined."""
    service = ModelService.get_instance()

    # Case 1: Missing required column
    f_missing = dict(valid_features)
    del f_missing["amount"]
    req_missing = PredictionRequest(
        model_name="test", model_version="1.0", features=f_missing
    )
    resp_missing = service.predict_single(req_missing)
    assert resp_missing.validation_status == "quarantined"
    assert "Missing required feature columns" in resp_missing.error_reason

    # Case 2: Negative amount
    f_neg = dict(valid_features)
    f_neg["amount"] = -100.0
    req_neg = PredictionRequest(
        model_name="test", model_version="1.0", features=f_neg
    )
    resp_neg = service.predict_single(req_neg)
    assert resp_neg.validation_status == "quarantined"
    assert "non-negative" in resp_neg.error_reason

    # Case 3: NaN in numeric feature
    f_nan = dict(valid_features)
    f_nan["oldbalanceOrg"] = float("nan")
    req_nan = PredictionRequest(
        model_name="test", model_version="1.0", features=f_nan
    )
    resp_nan = service.predict_single(req_nan)
    assert resp_nan.validation_status == "quarantined"
    assert "finite" in resp_nan.error_reason


def test_model_service_unseen_category_handling(valid_features):
    """Verify unseen category levels (e.g. 'CRYPTO') pass without crashing and without refitting."""
    service = ModelService.get_instance()
    f_unseen = dict(valid_features)
    f_unseen["type"] = "CRYPTO"  # Never observed in training

    req = PredictionRequest(
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        features=f_unseen,
    )
    resp = service.predict_single(req)

    assert resp.validation_status == "valid"
    assert resp.prediction in (0, 1)
    assert 0.0 <= resp.score <= 1.0


def test_model_service_missing_artifacts_failure():
    """Verify service raises FileNotFoundError if model artifacts directory is invalid."""
    with pytest.raises(FileNotFoundError) as exc_info:
        svc = ModelService(artifacts_dir="non_existent_artifacts_path")
        svc.load_artifacts()
    assert "Model artifact not found" in str(exc_info.value)


# ──────────────────────────────────────────────────────────────────────────────
# 2. MONITORING ORCHESTRATION TESTS
# ──────────────────────────────────────────────────────────────────────────────

def test_monitoring_reference_immutability(test_repo):
    """Verify reference dataset is strictly immutable during a monitoring run."""
    monitoring_svc = MonitoringService(repository=test_repo)
    ref_before = monitoring_svc.get_reference_data()

    req = MonitoringRunRequest(
        scenario_id="numeric_mean_variance_shift",
        scenario_seed=42,
        scenario_samples=300,
    )
    monitoring_svc.execute_monitoring_run(req, persist=False)

    ref_after = monitoring_svc.get_reference_data()
    pd.testing.assert_frame_equal(ref_before, ref_after)


def test_monitoring_no_drift_scenario(test_repo):
    """Verify monitoring run under no_drift scenario produces Low severity and status 'ok'."""
    monitoring_svc = MonitoringService(repository=test_repo)
    req = MonitoringRunRequest(
        scenario_id="no_drift",
        scenario_seed=42,
        scenario_samples=500,
    )
    res = monitoring_svc.execute_monitoring_run(req, persist=True)

    assert isinstance(res, MonitoringRunResponse)
    assert res.status == "ok"
    assert res.dataset_drift_detected is False
    assert res.severity.band == "Low"
    assert res.severity.score < 25
    assert len(res.features) == 6


def test_monitoring_numeric_drift_scenario(test_repo):
    """Verify monitoring run detects numeric shift and identifies top contributor."""
    monitoring_svc = MonitoringService(repository=test_repo)
    req = MonitoringRunRequest(
        scenario_id="numeric_mean_variance_shift",
        scenario_seed=42,
        scenario_samples=500,
    )
    res = monitoring_svc.execute_monitoring_run(req, persist=True)

    assert res.dataset_drift_detected is True
    assert len(res.top_contributors) >= 1
    top_feature_names = [tc.feature_name for tc in res.top_contributors]
    assert "amount" in top_feature_names or "oldbalanceOrg" in top_feature_names
    assert res.severity.components["D"] > 0.0


def test_monitoring_data_quality_failures_scenario(test_repo):
    """Verify data quality corruption triggers quality findings and component Q."""
    monitoring_svc = MonitoringService(repository=test_repo)
    req = MonitoringRunRequest(
        scenario_id="data_quality_failures",
        scenario_seed=42,
        scenario_samples=500,
    )
    res = monitoring_svc.execute_monitoring_run(req, persist=True)

    # Component Q should reflect the injected nulls and negatives
    assert res.severity.components["Q"] is not None
    assert res.severity.components["Q"] > 50.0
    assert any("Data quality" in rec for rec in res.recommendations)


def test_monitoring_delayed_labels_renormalisation(test_repo):
    """Verify absent ground truth labels leaves M as None with renormalized weights."""
    monitoring_svc = MonitoringService(repository=test_repo)
    req = MonitoringRunRequest(
        scenario_id="no_drift",
        scenario_seed=42,
        scenario_samples=300,
        ground_truth_labels=None,  # Delayed labels
    )
    res = monitoring_svc.execute_monitoring_run(req, persist=True)

    assert res.severity.components["M"] is None
    assert res.severity.weights_used.get("M", 0.0) == 0.0
    active_weight_sum = sum(res.severity.weights_used.values())
    assert active_weight_sum == pytest.approx(1.0, abs=1e-5)
    assert any("Ground-truth labels unavailable" in rec for rec in res.recommendations)


def test_monitoring_with_ground_truth_labels(test_repo):
    """Verify ground truth labels allow evaluation of Component M."""
    monitoring_svc = MonitoringService(repository=test_repo)
    # Synthetic batch with labels
    ref_df = monitoring_svc.get_reference_data().iloc[:200]
    labels = [0] * 195 + [1] * 5

    req = MonitoringRunRequest(
        current_data=ref_df.to_dict(orient="records"),
        ground_truth_labels=labels,
    )
    res = monitoring_svc.execute_monitoring_run(req, persist=True)

    assert res.severity.components["M"] is not None
    assert 0.0 <= res.severity.components["M"] <= 100.0


def test_monitoring_reproducibility_across_identical_runs(test_repo):
    """Verify identical monitoring requests produce bit-for-bit identical metrics."""
    monitoring_svc = MonitoringService(repository=test_repo)
    req1 = MonitoringRunRequest(
        scenario_id="numeric_mean_variance_shift",
        scenario_seed=42,
        scenario_samples=400,
    )
    res1 = monitoring_svc.execute_monitoring_run(req1, persist=False)

    req2 = MonitoringRunRequest(
        scenario_id="numeric_mean_variance_shift",
        scenario_seed=42,
        scenario_samples=400,
    )
    res2 = monitoring_svc.execute_monitoring_run(req2, persist=False)

    assert res1.severity.score == res2.severity.score
    assert res1.severity.band == res2.severity.band
    assert res1.prediction_drift.value == res2.prediction_drift.value
    assert len(res1.features) == len(res2.features)


# ──────────────────────────────────────────────────────────────────────────────
# 3. PERSISTENCE REPOSITORY TESTS (IN-MEMORY & SQLITE)
# ──────────────────────────────────────────────────────────────────────────────

def test_inmemory_repository_crud():
    """Verify InMemoryMonitoringRepository save, get, list, count, and duplicate handling."""
    repo = InMemoryMonitoringRepository()
    svc = MonitoringService(repository=repo)
    req = MonitoringRunRequest(scenario_id="no_drift", scenario_samples=100)
    run_resp = svc.execute_monitoring_run(req, persist=True)

    # Retrieval
    retrieved = repo.get_run(run_resp.run_id)
    assert retrieved is not None
    assert retrieved.run_id == run_resp.run_id
    assert retrieved.severity.score == run_resp.severity.score

    # Pagination & count
    assert repo.count_runs() == 1
    runs_list = repo.list_runs(limit=10, offset=0)
    assert len(runs_list) == 1

    # Duplicate ID rejection
    with pytest.raises(ValueError) as exc:
        repo.save_run(run_resp)
    assert "Duplicate run_id" in str(exc.value)

    # Simulated outage behavior
    repo.simulate_failure = True
    with pytest.raises(RuntimeError) as exc_fail:
        svc.execute_monitoring_run(req, persist=True)
    assert "Simulated database outage" in str(exc_fail.value)


def test_sqlite_repository_acid_transactions():
    """Verify SQLiteMonitoringRepository persists and retrieves records with ACID transactions."""
    sqlite_repo = SQLiteMonitoringRepository(db_path=":memory:")
    svc = MonitoringService(repository=sqlite_repo)
    req = MonitoringRunRequest(scenario_id="numeric_mean_variance_shift", scenario_samples=200)
    run_resp = svc.execute_monitoring_run(req, persist=True)

    # Retrieve from SQLite
    retrieved = sqlite_repo.get_run(run_resp.run_id)
    assert retrieved is not None
    assert retrieved.run_id == run_resp.run_id
    assert retrieved.severity.score == run_resp.severity.score
    assert retrieved.dataset_drift_detected is True
    assert sqlite_repo.count_runs() == 1

    # Duplicate insertion raises ValueError
    with pytest.raises(ValueError) as exc:
        sqlite_repo.save_run(run_resp)
    assert "Duplicate run_id" in str(exc.value)

    # Simulated failure handling
    sqlite_repo.simulate_failure = True
    with pytest.raises(RuntimeError) as exc_fail:
        svc.execute_monitoring_run(req, persist=True)
    assert "Simulated SQLite database failure" in str(exc_fail.value)


# ──────────────────────────────────────────────────────────────────────────────
# 4. FASTAPI ENDPOINT INTEGRATION TESTS
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_api_health_endpoint():
    """Verify GET /health returns service health, version, and model status."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["model_loaded"] is True
        assert data["model_name"] == "paysim-fraud-histgradientboost"


@pytest.mark.asyncio
async def test_api_predict_single_endpoint(valid_features):
    """Verify POST /predict executes inference via HTTP API."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "model_name": "paysim-fraud-histgradientboost",
            "model_version": "1.0.0",
            "features": valid_features,
        }
        resp = await client.post("/predict", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["validation_status"] == "valid"
        assert data["prediction"] in (0, 1)
        assert 0.0 <= data["score"] <= 1.0


@pytest.mark.asyncio
async def test_api_predict_batch_endpoint(valid_features):
    """Verify POST /predict/batch executes vectorized batch inference."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "model_name": "paysim-fraud-histgradientboost",
            "model_version": "1.0.0",
            "predictions": [
                {"model_name": "test", "model_version": "1.0", "features": valid_features},
                {"model_name": "test", "model_version": "1.0", "features": valid_features},
            ],
        }
        resp = await client.post("/predict/batch", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        assert len(data["results"]) == 2


@pytest.mark.asyncio
async def test_api_monitoring_runs_workflow():
    """Verify POST /monitoring/runs, GET /monitoring/runs, and GET /monitoring/runs/{id}."""
    repo = InMemoryMonitoringRepository()
    set_monitoring_repository(repo)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Create run via API
        payload = {
            "scenario_id": "no_drift",
            "scenario_seed": 42,
            "scenario_samples": 200,
        }
        post_resp = await client.post("/monitoring/runs", json=payload)
        assert post_resp.status_code == 201
        run_data = post_resp.json()
        run_id = run_data["run_id"]
        assert run_id is not None
        assert run_data["status"] == "ok"

        # 2. List runs via API
        list_resp = await client.get("/monitoring/runs?limit=10&offset=0")
        assert list_resp.status_code == 200
        runs_list = list_resp.json()
        assert len(runs_list) >= 1
        assert any(r["run_id"] == run_id for r in runs_list)

        # 3. Get individual run by ID
        get_resp = await client.get(f"/monitoring/runs/{run_id}")
        assert get_resp.status_code == 200
        single_data = get_resp.json()
        assert single_data["run_id"] == run_id
        assert single_data["severity"]["score"] == run_data["severity"]["score"]

        # 4. Get non-existent run returns 404
        bad_resp = await client.get("/monitoring/runs/non-existent-uuid")
        assert bad_resp.status_code == 404


# ──────────────────────────────────────────────────────────────────────────────
# 5. INDEPENDENT AUDIT CALCULATIONS
# ──────────────────────────────────────────────────────────────────────────────

def test_independent_prediction_drift_audit():
    """Independently audit prediction score PSI calculation against mathematical formula."""
    from backend.app.services.drift_metrics import compute_numeric_psi

    # Fix synthetic prediction probability distributions
    np.random.seed(42)
    ref_scores = np.random.beta(a=0.5, b=5.0, size=1000)
    cur_scores = np.random.beta(a=1.5, b=3.0, size=1000)  # Drifted higher

    # Library calculation
    metric_res = compute_numeric_psi(ref_scores, cur_scores, n_bins=10, threshold=0.25)

    # Independent mathematical computation
    quantiles = np.linspace(0, 1, 11)
    bin_edges = np.quantile(ref_scores, quantiles)
    bin_edges[0] = -np.inf
    bin_edges[-1] = np.inf

    ref_counts = np.histogram(ref_scores, bins=bin_edges)[0]
    cur_counts = np.histogram(cur_scores, bins=bin_edges)[0]

    ref_props = (ref_counts + 1e-4) / (len(ref_scores) + 1e-4 * 10)
    cur_props = (cur_counts + 1e-4) / (len(cur_scores) + 1e-4 * 10)

    independent_psi = float(np.sum((cur_props - ref_props) * np.log(cur_props / ref_props)))

    assert abs(metric_res.value - independent_psi) < 1e-3
    assert metric_res.exceeded == (independent_psi >= 0.25)


def test_severity_results_match_phase3_calculation(test_repo):
    """Verify monitoring run severity matches independent Phase 3 multi-component calculation."""
    monitoring_svc = MonitoringService(repository=test_repo)
    req = MonitoringRunRequest(
        scenario_id="numeric_mean_variance_shift",
        scenario_seed=42,
        scenario_samples=500,
    )
    res = monitoring_svc.execute_monitoring_run(req, persist=False)

    comps = res.severity.components
    weights = res.severity.weights_used

    # Independent scalar aggregation over available components
    avail_comps = {k: v for k, v in comps.items() if v is not None}
    expected_raw = sum(weights[k] * avail_comps[k] for k in avail_comps)
    expected_int = int(round(expected_raw))

    assert res.severity.score == expected_int
    assert sum(weights.values()) == pytest.approx(1.0, abs=1e-5)
