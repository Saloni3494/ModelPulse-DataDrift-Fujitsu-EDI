"""ModelPulse Model Inference API Routes.
Provides /predict and /predict/batch endpoints with schema validation and quarantine support.
"""
from fastapi import APIRouter, HTTPException, status
from backend.app.schemas.predictions import (
    PredictionRequest,
    PredictionResponse,
    BatchPredictionRequest,
    BatchPredictionResponse,
)
from backend.app.services.model_service import ModelService

router = APIRouter(tags=["inference"])


@router.post(
    "/predict",
    response_model=PredictionResponse,
    summary="Predict transaction fraud probability",
    description="Score a single transaction. Validates schema and quarantined records.",
)
async def predict_single(request: PredictionRequest) -> PredictionResponse:
    """Execute single-transaction fraud probability scoring."""
    try:
        service = ModelService.get_instance()
        response = service.predict_single(request)
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
    """Execute vectorized batch fraud scoring with per-record quarantine support."""
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
