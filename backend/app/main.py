"""ModelPulse FastAPI Application Entrypoint.
Phase 4: Integrates inference API, monitoring orchestration, and health checks.
"""
from fastapi import FastAPI
from backend.app.core.config import get_settings
from backend.app.api.predictions import router as predictions_router
from backend.app.api.monitoring import router as monitoring_router
from backend.app.services.model_service import ModelService

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    description="MLOps Model & Data Drift Monitoring Platform (ModelPulse v2)",
    version="0.2.0",
)

# Register route modules
app.include_router(predictions_router)
app.include_router(monitoring_router)


@app.get("/health", tags=["system"])
async def health_check():
    """Health check endpoint confirming service, environment, and model artifact status."""
    model_loaded = False
    model_name = "paysim-fraud-histgradientboost"
    model_version = "1.0.0"
    try:
        service = ModelService.get_instance()
        model_loaded = service._loaded
        model_name = service.model_name
        model_version = service.model_version
    except Exception:
        model_loaded = False

    return {
        "status": "healthy",
        "app_name": settings.APP_NAME,
        "environment": settings.APP_ENV,
        "version": "0.1.0",
        "model_loaded": model_loaded,
        "model_name": model_name,
        "model_version": model_version,
    }
