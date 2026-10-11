"""ModelPulse Dashboard Backend Client (Phase 6).
Provides dual-mode data access:
1. REST API Client connecting to the live FastAPI server.
2. Local Repository Fallback accessing storage and metrics directly when API server is offline.
"""
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional
import httpx

from backend.app.db.repository import get_monitoring_repository
from backend.app.schemas.monitoring import MonitoringRunResponse
from backend.app.streaming.metrics import get_streaming_metrics
from backend.app.alerting.service import get_alert_service
from backend.app.core.config import get_settings

logger = logging.getLogger(__name__)


class DashboardClient:
    """Unified client for Streamlit dashboard fetching monitoring, drift, streaming, and alert data."""

    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = 5.0
        self.settings = get_settings()
        self.last_data_source: str = "uninitialized"
        self.last_api_error: Optional[str] = None

    def _inspect_local_repository(self) -> Dict[str, Any]:
        """Inspect the local storage engine, checking file existence, freshness, and multi-process isolation."""
        import os
        ptype = self.settings.PERSISTENCE_TYPE.lower()

        if ptype == "sqlite":
            db_path = self.settings.SQLITE_DB_PATH
            exists = os.path.exists(db_path)
            mtime_str = None
            if exists:
                try:
                    mtime = os.path.getmtime(db_path)
                    mtime_str = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
                except Exception:
                    pass

            run_count = 0
            try:
                repo = get_monitoring_repository()
                run_count = repo.count_runs()
            except Exception:
                pass

            warning = None
            if not exists:
                warning = f"SQLite database '{db_path}' does not exist on disk. No runs recorded."
            elif run_count == 0:
                warning = f"SQLite database '{db_path}' exists but contains 0 monitoring runs."

            return {
                "data_source": f"Local SQLite ({db_path})",
                "data_source_detail": f"SQLite database: {db_path} (exists={exists}, runs={run_count})",
                "is_shared_storage": True,
                "storage_path": db_path,
                "storage_exists": exists,
                "run_count": run_count,
                "last_modified": mtime_str,
                "freshness_warning": warning,
            }
        else:
            # in_memory mode: isolated per process
            run_count = 0
            try:
                repo = get_monitoring_repository()
                run_count = repo.count_runs()
            except Exception:
                pass

            return {
                "data_source": "Local In-Memory (Process Isolated)",
                "data_source_detail": "Process-local in-memory store (unshared with FastAPI)",
                "is_shared_storage": False,
                "storage_path": None,
                "storage_exists": True,
                "run_count": run_count,
                "last_modified": None,
                "freshness_warning": (
                    "DISCONNECTED / UNCACHED: The FastAPI backend is unreachable, and in_memory mode "
                    "cannot share data across processes. This Streamlit instance has an empty/isolated "
                    "store. Configure PERSISTENCE_TYPE=sqlite or start FastAPI to view shared history."
                ),
            }

    def get_backend_status(self) -> Dict[str, Any]:
        """Check API connectivity, error category, and active repository fallback mode."""
        api_error_type: Optional[str] = None
        api_error_detail: Optional[str] = None

        try:
            with httpx.Client(timeout=2.0) as client:
                resp = client.get(f"{self.api_base_url}/health")
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        self.last_data_source = "api"
                        self.last_api_error = None
                        return {
                            "mode": "api",
                            "status": "connected",
                            "url": self.api_base_url,
                            "api_error": None,
                            "data_source": "FastAPI REST API",
                            "data_source_detail": f"Connected to {self.api_base_url}",
                            "details": data,
                            "persistence_type": self.settings.PERSISTENCE_TYPE,
                            "is_shared_storage": True,
                            "freshness_warning": None,
                        }
                    except Exception as json_err:
                        api_error_type = "malformed_json"
                        api_error_detail = f"Malformed JSON from /health: {json_err}"
                else:
                    api_error_type = f"http_{resp.status_code}"
                    api_error_detail = f"HTTP {resp.status_code}: {resp.text}"
        except httpx.ConnectError as conn_err:
            api_error_type = "connection_refused"
            api_error_detail = f"Connection refused to {self.api_base_url}: {conn_err}"
        except httpx.TimeoutException as time_err:
            api_error_type = "timeout"
            api_error_detail = f"Request timed out after 2.0s connecting to {self.api_base_url}: {time_err}"
        except Exception as exc:
            api_error_type = "error"
            api_error_detail = f"API check failed: {exc}"

        # API is offline or returned an error -> inspect local repository fallback
        self.last_api_error = api_error_detail
        repo_info = self._inspect_local_repository()
        self.last_data_source = repo_info["data_source"]

        return {
            "mode": "direct_local",
            "status": "active_local_fallback",
            "failure_reason": api_error_type,
            "api_error": api_error_detail,
            "url": None,
            "data_source": repo_info["data_source"],
            "data_source_detail": repo_info["data_source_detail"],
            "persistence_type": self.settings.PERSISTENCE_TYPE,
            "is_shared_storage": repo_info["is_shared_storage"],
            "storage_path": repo_info.get("storage_path"),
            "storage_exists": repo_info.get("storage_exists"),
            "storage_run_count": repo_info.get("run_count", 0),
            "storage_last_modified": repo_info.get("last_modified"),
            "freshness_warning": repo_info.get("freshness_warning"),
            "details": {
                "status": "offline_fallback",
                "app_name": self.settings.APP_NAME,
                "environment": self.settings.APP_ENV,
                "api_error": api_error_detail,
            },
        }

    def list_runs(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """List historic monitoring runs."""
        # Try REST API first
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.get(
                    f"{self.api_base_url}/monitoring/runs",
                    params={"limit": limit, "offset": offset},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list):
                        self.last_data_source = "api"
                        return data
                    else:
                        logger.warning(f"[DashboardClient] API returned non-list for /monitoring/runs: {type(data)}")
                else:
                    logger.warning(f"[DashboardClient] API /monitoring/runs returned HTTP {resp.status_code}")
        except Exception as exc:
            logger.debug(f"[DashboardClient] API call failed, falling back to local repository: {exc}")

        # Local repository fallback
        try:
            repo = get_monitoring_repository()
            runs = repo.list_runs(limit=limit, offset=offset)
            self.last_data_source = (
                f"local_sqlite ({self.settings.SQLITE_DB_PATH})"
                if self.settings.PERSISTENCE_TYPE == "sqlite"
                else "local_in_memory_isolated"
            )
            return [r.model_dump() for r in runs]
        except Exception as exc:
            logger.error(f"[DashboardClient] Local fallback repository error: {exc}")
            self.last_data_source = "error_fallback_unavailable"
            return []

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve detailed run report by run_id."""
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.get(f"{self.api_base_url}/monitoring/runs/{run_id}")
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        return data
        except Exception as exc:
            logger.debug(f"[DashboardClient] API get_run failed: {exc}")

        # Local repository fallback
        try:
            repo = get_monitoring_repository()
            run = repo.get_run(run_id)
            return run.model_dump() if run else None
        except Exception as exc:
            logger.error(f"[DashboardClient] Local fallback get_run error for {run_id}: {exc}")
            return None

    def get_streaming_health(self) -> Dict[str, Any]:
        """Fetch streaming telemetry counters and reconciliation audit."""
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.get(f"{self.api_base_url}/monitoring/streaming-health")
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            pass

        # Local streaming metrics fallback
        try:
            metrics = get_streaming_metrics()
            return {
                "summary": metrics.get_summary(),
                "reconciliation": metrics.get_reconciliation_table(),
            }
        except Exception as exc:
            logger.error(f"[DashboardClient] Error retrieving streaming metrics: {exc}")
            return {"summary": {}, "reconciliation": {}}

    def get_alert_status(self) -> Dict[str, Any]:
        """Fetch alert configuration and channel readiness."""
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.get(f"{self.api_base_url}/monitoring/alerts/status")
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            pass

        try:
            service = get_alert_service()
            return service.get_channel_status()
        except Exception as exc:
            logger.error(f"[DashboardClient] Error retrieving alert status: {exc}")
            return {"slack": {"configured": False}, "email": {"configured": False}}

    def list_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch recent alert notification records."""
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.get(
                    f"{self.api_base_url}/monitoring/alerts",
                    params={"limit": limit},
                )
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            pass

        try:
            service = get_alert_service()
            records = service.get_history(limit=limit)
            return [r.model_dump() for r in records]
        except Exception as exc:
            logger.error(f"[DashboardClient] Error listing alerts: {exc}")
            return []
