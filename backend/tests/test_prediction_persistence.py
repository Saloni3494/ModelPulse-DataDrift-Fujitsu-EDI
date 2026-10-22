
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.predictions import router, prediction_repository
from backend.app.schemas.predictions import PredictionResponse


app = FastAPI()
app.include_router(router)


def test_prediction_is_persisted():
    request_data = {
        "event_id": "persistence-test-001",
        "model_name": "fraud-detector",
        "model_version": "v1",
        "features": {
            "amount": 2500,
            "type": "TRANSFER",
        },
    }

    response_data = {
        **request_data,
        "prediction": 1,
        "score": 0.97,
        "validation_status": "valid",
        "timestamp": "2026-10-19T12:00:00+05:30",
        "error_reason": None,
    }

    mock_service = MagicMock()
    mock_service.predict_single.return_value = PredictionResponse(**response_data)

    with patch(
        "backend.app.api.predictions.ModelService.get_instance",
        return_value=mock_service,
    ):
        with TestClient(app) as client:
            response = client.post("/predict", json=request_data)

    print("Response body:", response.text)
    assert response.status_code == 200

    saved = prediction_repository.get_prediction("persistence-test-001")
    assert saved is not None
    assert saved["event_id"] == "persistence-test-001"
    assert saved["score"] == 0.97
