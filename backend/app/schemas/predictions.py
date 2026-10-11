"""Prediction schemas proposed in Phase 0 audit.
These schemas represent proposed contracts for Phase 2 validation.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field


class PredictionRequest(BaseModel):
    """Single prediction request contract."""

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique event identifier for tracking and label joining",
    )
    model_name: str = Field(..., description="Target registered model name")
    model_version: str = Field(..., description="Target model version")
    features: Dict[str, Any] = Field(..., description="Feature key-value pairs")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC event occurrence timestamp",
    )


class BatchPredictionRequest(BaseModel):
    """Batch prediction request contract."""

    model_name: str
    model_version: str
    predictions: List[PredictionRequest]


class PredictionResponse(BaseModel):
    """Single prediction response contract."""

    event_id: str
    model_name: str
    model_version: str
    prediction: Optional[int] = Field(
        default=None, description="Predicted class or label (None if quarantined)"
    )
    score: Optional[float] = Field(
        default=None, description="Model probability or confidence score (None if quarantined)"
    )
    validation_status: str = Field(
        default="valid", description="'valid' or 'quarantined'"
    )
    error_reason: Optional[str] = None
    timestamp: datetime


class BatchPredictionResponse(BaseModel):
    """Batch prediction response contract."""

    model_name: str
    model_version: str
    count: int
    results: List[PredictionResponse]
