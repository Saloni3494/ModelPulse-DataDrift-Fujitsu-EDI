
"""ModelPulse Model Inference API Routes.

Provides /predict and /predict/batch endpoints with schema validation,
quarantine support, and persistent prediction-event storage.
"""
from fastapi import APIRouter, HTTPException, status

from backend.app.schemas.predictions import (
    PredictionRequest,
    PredictionResponse,
    BatchPredictionRequest,
    BatchPredictionResponse,
)
from backend.app.services.model_service import ModelService
from backend.app.db.prediction_repository import PredictionRepository
from backend.app.schemas.events import TransactionEvent
from backend.app.streaming.producer import TransactionProducer

router = APIRouter(tags=["inference"])

# SQLite-backed prediction-event persistence.
prediction_repository = PredictionRepository()


def persist_prediction(
    request: PredictionRequest,
    response: PredictionResponse,
) -> None:
    """Persist a prediction while preserving request and response fields."""
    event = request.model_dump(mode="json")
    event.update(response.model_dump(mode="json"))

    prediction_repository.save_prediction(event)


@router.post(
    "/predict",
    response_model=PredictionResponse,
    summary="Predict transaction fraud probability",
    description="Score a single transaction. Validates schema and quarantined records.",
)
async def predict_single(request: PredictionRequest) -> PredictionResponse:
    """Score a transaction and persist its prediction event."""
    try:
        service = ModelService.get_instance()
        response = service.predict_single(request)

        persist_prediction(request, response)

        event = TransactionEvent(
            event_id=request.event_id,
            timestamp=request.timestamp,
            features=request.features,
            metadata={
                "model_name": request.model_name,
                "model_version": request.model_version,
                "prediction": response.prediction,
                "score": response.score,
                "validation_status": response.validation_status,
                "error_reason": response.error_reason,
            },
        )

        producer = TransactionProducer()
        await producer.publish_event(event)

        return response

    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Inference execution failed: {exc}",
        ) from exc


@router.post(
    "/predict/batch",
    response_model=BatchPredictionResponse,
    summary="Batch transaction fraud probability scoring",
    description="Score a collection of transactions with vectorized execution.",
)
async def predict_batch(
    request: BatchPredictionRequest,
) -> BatchPredictionResponse:
    """Execute vectorized batch scoring with per-record quarantine support."""
    try:
        service = ModelService.get_instance()
        response = service.predict_batch(request)
        return response

    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch inference execution failed: {exc}",
        ) from exc
