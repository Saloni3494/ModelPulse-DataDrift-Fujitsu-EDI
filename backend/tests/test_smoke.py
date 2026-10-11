"""ModelPulse Smoke Tests for Phase 0.
Verifies project modules, schemas, configuration, and FastAPI app importability.
"""
from datetime import datetime, timezone
import pytest
from httpx import ASGITransport, AsyncClient

# 1. Module Imports Verification
def test_core_dependencies_importable():
    """Verify core numerical and framework libraries import cleanly."""
    import numpy as np
    import pandas as pd
    import scipy
    import sklearn
    import pydantic
    import fastapi

    assert np.__version__ is not None
    assert pd.__version__ is not None
    assert scipy.__version__ is not None
    assert sklearn.__version__ is not None
    assert pydantic.__version__ is not None
    assert fastapi.__version__ is not None


def test_project_subpackages_importable():
    """Verify all top-level ModelPulse packages can be imported."""
    import backend.app
    import backend.app.core
    import backend.app.schemas
    import backend.app.api
    import backend.app.services
    import backend.app.streaming
    import backend.app.db
    import backend.app.jobs
    import ml
    import simulator
    import dashboard

    assert backend.app.__version__ == "0.1.0"


# 2. Configuration Verification
def test_settings_load_defaults():
    """Verify application settings initialize with expected Phase 0 defaults."""
    from backend.app.core.config import get_settings

    settings = get_settings()
    assert settings.APP_NAME == "ModelPulse"
    assert settings.DRIFT_ENSEMBLE_K == 2
    assert settings.DRIFT_ENSEMBLE_M == 3
    assert settings.MICROBATCH_MINUTES == 5
    assert settings.MONITOR_CYCLE_MINUTES == 60


# 3. Proposed Schema Contract Serialization
def test_prediction_schemas_serialization():
    """Verify PredictionRequest and PredictionResponse instantiate and serialize."""
    from backend.app.schemas.predictions import (
        PredictionRequest,
        PredictionResponse,
        BatchPredictionRequest,
    )

    req = PredictionRequest(
        model_name="fraud-detector",
        model_version="1.0.0",
        features={"amount": 250.0, "type": "TRANSFER", "oldbalanceOrg": 1000.0},
    )
    assert req.event_id is not None
    assert req.features["amount"] == 250.0

    batch_req = BatchPredictionRequest(
        model_name="fraud-detector",
        model_version="1.0.0",
        predictions=[req],
    )
    assert len(batch_req.predictions) == 1

    resp = PredictionResponse(
        event_id=req.event_id,
        model_name=req.model_name,
        model_version=req.model_version,
        prediction=0,
        score=0.04,
        validation_status="valid",
        timestamp=datetime.now(timezone.utc),
    )
    assert resp.prediction == 0
    assert resp.score == 0.04


def test_drift_and_monitoring_schemas():
    """Verify FeatureDriftResult and MonitoringRunResponse instantiate and serialize."""
    from backend.app.schemas.drift import TestResult, FeatureDriftResult
    from backend.app.schemas.monitoring import (
        WindowSpec,
        PredictionDriftSummary,
        SeverityBreakdown,
        TopContributor,
        MonitoringRunResponse,
    )

    test_psi = TestResult(
        metric="psi", value=0.28, threshold=0.25, exceeded=True
    )
    test_ks = TestResult(
        metric="ks", value=0.15, threshold=0.10, p_value=0.002, exceeded=True
    )
    test_wass = TestResult(
        metric="wasserstein", value=0.18, threshold=0.30, exceeded=False
    )

    feature_res = FeatureDriftResult(
        feature_name="amount",
        feature_type="numeric",
        tests=[test_psi, test_ks, test_wass],
        votes="2/3",
        votes_count=2,
        total_tests=3,
        drift_detected=True,
        drift_contribution=0.34,
    )
    assert feature_res.drift_detected is True
    assert feature_res.votes_count == 2

    now = datetime.now(timezone.utc)
    run_resp = MonitoringRunResponse(
        run_id="run-test-123",
        model_name="fraud-detector",
        model_version="1.0.0",
        run_type="hourly",
        reference_window=WindowSpec(start=now, end=now, sample_count=5000),
        current_window=WindowSpec(start=now, end=now, sample_count=1200),
        sample_count=1200,
        status="ok",
        dataset_drift_detected=True,
        prediction_drift=PredictionDriftSummary(
            metric="psi", value=0.12, threshold=0.25, drifted=False
        ),
        severity=SeverityBreakdown(
            score=58,
            band="High",
            confidence="normal",
            components={"D": 65.0, "P": 25.0, "Q": 0.0, "M": None},
            weights_used={"D": 0.53, "P": 0.27, "Q": 0.20},
        ),
        features=[feature_res],
        top_contributors=[
            TopContributor(
                feature_name="amount",
                contribution_score=0.34,
                cumulative_share=0.45,
            )
        ],
        recommendations=["Investigate feature 'amount' for covariate shift"],
        created_at=now,
    )
    assert run_resp.severity.score == 58
    assert run_resp.severity.band == "High"


# 4. FastAPI Endpoint Integration
@pytest.mark.asyncio
async def test_health_endpoint():
    """Verify GET /health returns 200 OK and healthy status."""
    from backend.app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["app_name"] == "ModelPulse"
