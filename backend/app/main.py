
"""ModelPulse FastAPI Application Entrypoint."""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.app.core.config import get_settings
from backend.app.api.predictions import router as predictions_router
from backend.app.api.monitoring import router as monitoring_router
from backend.app.services.model_service import ModelService
from backend.app.streaming.bus import get_message_bus
from backend.app.streaming.consumer import StreamingConsumer
from typing import Literal, Optional

from pydantic import BaseModel, Field

from backend.app.streaming.producer import (
    DeterministicReplayService,
    TransactionProducer,
)

class ReplayRequest(BaseModel):
    scenario_id: Literal[
        "no_drift",
        "numeric_mean_variance_shift",
        "categorical_proportion_shift",
        "unseen_categories",
        "sudden_drift",
        "gradual_drift",
        "data_quality_failures",
    ] = "numeric_mean_variance_shift"

    seed: int = 42
    n_samples: int = Field(default=100, ge=1, le=10000)
    rate_per_sec: Optional[float] = Field(default=None, gt=0)

settings = get_settings()



@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the streaming consumer with the API and stop it gracefully."""
    print("[DEBUG] FastAPI lifespan started", flush=True)

    message_bus = get_message_bus()
    consumer = StreamingConsumer(message_bus=message_bus)
    app.state.streaming_consumer = consumer

    consumer_task = asyncio.create_task(consumer.run_loop())
    await asyncio.sleep(0.2)

    print(f"[DEBUG] Consumer task started: {not consumer_task.done()}", flush=True)
    if consumer_task.done():
        print(f"[DEBUG] Consumer task result: {consumer_task.exception()}", flush=True)

    try:
        yield
    finally:
        await consumer.stop()
        await consumer_task



app = FastAPI(
    title=settings.APP_NAME,
    description="MLOps Model & Data Drift Monitoring Platform (ModelPulse v2)",
    version="0.2.0",
    lifespan=lifespan,
)

# Register route modules
app.include_router(predictions_router)
app.include_router(monitoring_router)


@app.get("/health", tags=["system"])
async def health_check():
    """Health check endpoint confirming service and model status."""
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

@app.post("/monitoring/streaming/replay", tags=["streaming"])
@app.post("/monitoring/replay", tags=["streaming"])
async def replay_stream(request: ReplayRequest):
    # Reuse the exact bus instance connected to the running consumer.
    consumer = app.state.streaming_consumer

    producer = TransactionProducer(
        topic=consumer.topic,
        message_bus=consumer.message_bus,
    )

    replay_service = DeterministicReplayService(
        producer=producer,
    )

    result = await replay_service.replay_scenario(
        scenario_id=request.scenario_id,
        seed=request.seed,
        n_samples=request.n_samples,
        rate_per_sec=request.rate_per_sec,
    )

    return result

@app.post("/monitoring/streaming/flush", tags=["streaming"])
@app.post("/monitoring/flush", tags=["streaming"])
async def flush_stream():
    """Flush buffered streaming events and run monitoring."""
    consumer = app.state.streaming_consumer

    result = await consumer.flush_and_commit(persist=True)

    if result is None:
        return {"status": "no_pending_batch"}

    meta, run_response = result

    return {
        "status": meta.status,
        "batch_id": meta.batch_id,
        "received_count": meta.received_count,
        "valid_count": meta.valid_count,
        "quarantined_count": meta.quarantined_count,
        "duplicate_count": meta.duplicate_count,
        "monitoring_run_id": (
            run_response.run_id if run_response else None
        ),
    }