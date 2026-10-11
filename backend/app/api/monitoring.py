"""ModelPulse Drift Monitoring API Routes.
Provides:
- POST /monitoring/runs (Execute and persist a monitoring cycle)
- GET /monitoring/runs (Paginated query of historic runs)
- GET /monitoring/runs/{run_id} (Retrieve individual run report)
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, status

from backend.app.schemas.monitoring import (
    MonitoringRunRequest,
    MonitoringRunResponse,
)
from backend.app.services.monitoring_service import MonitoringService
from backend.app.db.repository import get_monitoring_repository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.post(
    "/runs",
    response_model=MonitoringRunResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Execute and persist a drift monitoring run",
    description="Evaluates feature drift, prediction drift, data quality, and deterministic severity.",
)
async def create_monitoring_run(
    request: MonitoringRunRequest,
) -> MonitoringRunResponse:
    """Execute end-to-end drift monitoring run and persist results."""
    try:
        service = MonitoringService()
        response = service.execute_monitoring_run(request, persist=True)
        try:
            from backend.app.alerting.service import get_alert_service
            await get_alert_service().evaluate_and_dispatch(response)
        except Exception as alt_exc:
            logger.warning(f"[MonitoringAPI] Alert evaluation failed for run {response.run_id}: {alt_exc}")
        return response

    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Monitoring run execution failed: {exc}",
        ) from exc


@router.get(
    "/runs",
    response_model=List[MonitoringRunResponse],
    summary="List historic monitoring runs",
    description="Retrieve paginated list of monitoring run reports ordered by created timestamp descending.",
)
async def list_monitoring_runs(
    limit: int = Query(default=50, ge=1, le=500, description="Page size limit"),
    offset: int = Query(default=0, ge=0, description="Offset cursor for pagination"),
) -> List[MonitoringRunResponse]:
    """List monitoring runs with pagination."""
    repo = get_monitoring_repository()
    return repo.list_runs(limit=limit, offset=offset)


@router.get(
    "/runs/{run_id}",
    response_model=MonitoringRunResponse,
    summary="Retrieve monitoring run report by ID",
)
async def get_monitoring_run(run_id: str) -> MonitoringRunResponse:
    """Retrieve detailed monitoring run report by ID."""
    repo = get_monitoring_repository()
    run = repo.get_run(run_id)
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Monitoring run '{run_id}' not found.",
        )
    return run


@router.get(
    "/streaming-health",
    summary="Retrieve streaming telemetry and reconciliation stats",
    description="Returns live counters, processing durations, and mathematically audited event accounting.",
)
async def get_streaming_health():
    """Return streaming metrics and reconciliation table."""
    from backend.app.streaming.metrics import get_streaming_metrics
    metrics = get_streaming_metrics()
    return {
        "summary": metrics.get_summary(),
        "reconciliation": metrics.get_reconciliation_table(),
    }


@router.get(
    "/alerts",
    summary="List recent alert notification history",
)
async def list_alerts(
    limit: int = Query(default=50, ge=1, le=200, description="Max records to return"),
):
    """Retrieve audit history of recent alert delivery attempts."""
    from backend.app.alerting.service import get_alert_service
    service = get_alert_service()
    return service.get_history(limit=limit)


@router.get(
    "/alerts/status",
    summary="Retrieve alert channel configuration status",
)
async def get_alerts_status():
    """Retrieve Slack and Email configuration readiness."""
    from backend.app.alerting.service import get_alert_service
    service = get_alert_service()
    return service.get_channel_status()

