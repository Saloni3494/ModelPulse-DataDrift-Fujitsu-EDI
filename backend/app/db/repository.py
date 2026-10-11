"""ModelPulse Monitoring Persistence Foundation.
Defines the repository interface and implementations:
- MonitoringRepository (Protocol / ABC)
- InMemoryMonitoringRepository (Deterministic in-memory repository for unit testing)
- SQLiteMonitoringRepository (ACID-compliant SQLite repository requiring zero external infrastructure)
- Repository factory and lifecycle management.
"""
from abc import ABC, abstractmethod
import json
import sqlite3
import threading
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

from backend.app.schemas.monitoring import MonitoringRunResponse
from backend.app.alerting.models import AlertOutboxRecord, AlertOutboxStatus


class MonitoringRepository(ABC):
    """Abstract repository contract separating persistence from monitoring execution."""

    @abstractmethod
    def save_run(
        self,
        run: MonitoringRunResponse,
        outbox_records: Optional[List[AlertOutboxRecord]] = None,
    ) -> bool:
        """Persist a completed monitoring run report, optionally with outbox records atomically.

        Raises:
            ValueError: If duplicate run_id already exists.
            RuntimeError: If database connection or write fails.
        """
        pass

    @abstractmethod
    def get_run(self, run_id: str) -> Optional[MonitoringRunResponse]:
        """Retrieve a monitoring run by ID. Returns None if not found."""
        pass

    @abstractmethod
    def list_runs(
        self, limit: int = 50, offset: int = 0
    ) -> List[MonitoringRunResponse]:
        """List persisted monitoring runs with pagination ordered by created_at desc."""
        pass

    @abstractmethod
    def count_runs(self) -> int:
        """Return total count of persisted monitoring runs."""
        pass

    @abstractmethod
    def delete_run(self, run_id: str) -> bool:
        """Delete a monitoring run by ID."""
        pass

    @abstractmethod
    def save_alert_outbox(self, record: AlertOutboxRecord) -> bool:
        """Persist or update an alert outbox entry."""
        pass

    @abstractmethod
    def get_pending_alert_outbox(self, limit: int = 50) -> List[AlertOutboxRecord]:
        """Retrieve pending outbox entries ordered by created_at asc."""
        pass

    @abstractmethod
    def claim_pending_alert_outbox(
        self, limit: int = 50, worker_id: str = "worker-default", lease_seconds: int = 300
    ) -> List[AlertOutboxRecord]:
        """Atomically claim pending/unleased outbox entries to prevent duplicate dispatch across concurrent workers."""
        pass

    @abstractmethod
    def update_alert_outbox_status(
        self,
        outbox_id: str,
        status: str,
        error_message: Optional[str] = None,
        retry_count: Optional[int] = None,
        claim_token: Optional[int] = None,
    ) -> bool:
        """Update outbox entry status and failure diagnostics with optional fencing token validation."""
        pass

    @abstractmethod
    def reset_failed_outbox_record(self, outbox_id: str) -> bool:
        """Manually reset a permanently failed outbox record to pending for operator-initiated replay."""
        pass

    @abstractmethod
    def get_alert_outbox_record(self, outbox_id: str) -> Optional[AlertOutboxRecord]:
        """Retrieve outbox record by outbox_id."""
        pass


class InMemoryMonitoringRepository(MonitoringRepository):
    """Thread-safe, deterministic in-memory repository for isolated unit testing."""

    def __init__(self):
        self._storage: Dict[str, MonitoringRunResponse] = {}
        self._outbox: Dict[str, AlertOutboxRecord] = {}
        self._lock = threading.Lock()
        self.simulate_failure: bool = False

    def save_run(
        self,
        run: MonitoringRunResponse,
        outbox_records: Optional[List[AlertOutboxRecord]] = None,
    ) -> bool:
        with self._lock:
            if self.simulate_failure:
                raise RuntimeError(
                    "Simulated database outage: persistence transaction failed."
                )
            if run.run_id in self._storage:
                raise ValueError(
                    f"Duplicate run_id '{run.run_id}': record already exists in repository."
                )
            # Store deep copy via Pydantic model serialization
            self._storage[run.run_id] = MonitoringRunResponse.model_validate(
                run.model_dump()
            )
            if outbox_records:
                for rec in outbox_records:
                    self._outbox[rec.outbox_id] = AlertOutboxRecord.model_validate(
                        rec.model_dump()
                    )
            return True

    def get_run(self, run_id: str) -> Optional[MonitoringRunResponse]:
        with self._lock:
            run = self._storage.get(run_id)
            if run is None:
                return None
            return MonitoringRunResponse.model_validate(run.model_dump())

    def list_runs(
        self, limit: int = 50, offset: int = 0
    ) -> List[MonitoringRunResponse]:
        with self._lock:
            all_runs = sorted(
                self._storage.values(),
                key=lambda r: r.created_at,
                reverse=True,
            )
            sliced = all_runs[offset : offset + limit]
            return [
                MonitoringRunResponse.model_validate(r.model_dump())
                for r in sliced
            ]

    def count_runs(self) -> int:
        with self._lock:
            return len(self._storage)

    def delete_run(self, run_id: str) -> bool:
        with self._lock:
            if run_id in self._storage:
                del self._storage[run_id]
                return True
            return False

    def clear(self) -> None:
        """Clear all stored runs and outbox entries (testing utility)."""
        with self._lock:
            self._storage.clear()
            self._outbox.clear()

    def save_alert_outbox(self, record: AlertOutboxRecord) -> bool:
        with self._lock:
            existing = self._outbox.get(record.outbox_id)
            if existing is not None:
                exist_status = (
                    existing.status.value
                    if hasattr(existing.status, "value")
                    else str(existing.status)
                )
                if exist_status in (
                    "delivered",
                    "suppressed_cooldown",
                    "skipped_disabled",
                    "failed",
                    "in_flight",
                ):
                    # Do not overwrite terminal or in-flight records with pending!
                    return True
            self._outbox[record.outbox_id] = AlertOutboxRecord.model_validate(record.model_dump())
            return True

    def get_pending_alert_outbox(self, limit: int = 50) -> List[AlertOutboxRecord]:
        with self._lock:
            pending = [
                AlertOutboxRecord.model_validate(r.model_dump())
                for r in self._outbox.values()
                if (r.status.value if hasattr(r.status, "value") else str(r.status)) in ("pending", "retrying")
                and r.retry_count < r.max_retries
            ]
            pending.sort(key=lambda r: r.created_at)
            return pending[:limit]

    def claim_pending_alert_outbox(
        self, limit: int = 50, worker_id: str = "worker-default", lease_seconds: int = 300
    ) -> List[AlertOutboxRecord]:
        with self._lock:
            now = datetime.now(timezone.utc)
            claimed = []
            for r in sorted(self._outbox.values(), key=lambda x: x.created_at):
                stat = r.status.value if hasattr(r.status, "value") else str(r.status)
                is_expired_lease = (
                    stat == "in_flight"
                    and (now - r.updated_at).total_seconds() > lease_seconds
                )
                if (stat in ("pending", "retrying") or is_expired_lease) and r.retry_count < r.max_retries:
                    r.status = AlertOutboxStatus.IN_FLIGHT
                    r.claim_token = (getattr(r, "claim_token", 0) or 0) + 1
                    r.claimed_by = worker_id
                    r.updated_at = now
                    r.error_message = f"Claimed by {worker_id}"
                    claimed.append(AlertOutboxRecord.model_validate(r.model_dump()))
                    if len(claimed) >= limit:
                        break
            return claimed

    def update_alert_outbox_status(
        self,
        outbox_id: str,
        status: str,
        error_message: Optional[str] = None,
        retry_count: Optional[int] = None,
        claim_token: Optional[int] = None,
    ) -> bool:
        with self._lock:
            if outbox_id not in self._outbox:
                return False
            rec = self._outbox[outbox_id]
            current_stat = rec.status.value if hasattr(rec.status, "value") else str(rec.status)
            # Terminal protection: cannot update terminal records via standard update
            if current_stat in ("delivered", "suppressed_cooldown", "skipped_disabled", "failed"):
                return False
            # Fencing token check: if token provided, must match current claim_token
            if claim_token is not None and getattr(rec, "claim_token", 0) != claim_token:
                return False

            rec.status = AlertOutboxStatus(status) if isinstance(status, str) else status
            rec.updated_at = datetime.now(timezone.utc)
            if error_message is not None:
                rec.error_message = error_message
            if retry_count is not None:
                rec.retry_count = retry_count
            return True

    def reset_failed_outbox_record(self, outbox_id: str) -> bool:
        with self._lock:
            if outbox_id in self._outbox:
                rec = self._outbox[outbox_id]
                current_stat = rec.status.value if hasattr(rec.status, "value") else str(rec.status)
                if current_stat == "failed":
                    rec.status = AlertOutboxStatus.PENDING
                    rec.retry_count = 0
                    rec.error_message = "Manually reset for replay"
                    rec.updated_at = datetime.now(timezone.utc)
                    return True
            return False

    def get_alert_outbox_record(self, outbox_id: str) -> Optional[AlertOutboxRecord]:
        with self._lock:
            rec = self._outbox.get(outbox_id)
            return AlertOutboxRecord.model_validate(rec.model_dump()) if rec else None


class SQLiteMonitoringRepository(MonitoringRepository):
    """ACID-compliant SQLite storage providing local persistence without external servers."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.simulate_failure: bool = False
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS monitoring_runs (
                        run_id TEXT PRIMARY KEY,
                        model_name TEXT NOT NULL,
                        model_version TEXT NOT NULL,
                        run_type TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        sample_count INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        dataset_drift_detected INTEGER NOT NULL,
                        severity_score INTEGER NOT NULL,
                        severity_band TEXT NOT NULL,
                        raw_payload TEXT NOT NULL
                    );
                    """
                )
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS feature_drift_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL,
                        feature_name TEXT NOT NULL,
                        drift_detected INTEGER NOT NULL,
                        drift_contribution REAL NOT NULL,
                        FOREIGN KEY (run_id) REFERENCES monitoring_runs (run_id) ON DELETE CASCADE
                    );
                    """
                )
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_runs_created ON monitoring_runs (created_at DESC);"
                )
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS alert_outbox (
                        outbox_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        channel TEXT NOT NULL,
                        severity_band TEXT NOT NULL,
                        status TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        max_retries INTEGER NOT NULL DEFAULT 3,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        error_message TEXT,
                        claim_token INTEGER NOT NULL DEFAULT 0,
                        claimed_by TEXT,
                        FOREIGN KEY (run_id) REFERENCES monitoring_runs (run_id) ON DELETE CASCADE
                    );
                    """
                )
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_outbox_status ON alert_outbox (status, updated_at);"
                )
                # Schema migration for existing databases
                cursor = self._conn.execute("PRAGMA table_info(alert_outbox);")
                existing_cols = {row["name"] for row in cursor.fetchall()}
                if "claim_token" not in existing_cols:
                    self._conn.execute("ALTER TABLE alert_outbox ADD COLUMN claim_token INTEGER NOT NULL DEFAULT 0;")
                if "claimed_by" not in existing_cols:
                    self._conn.execute("ALTER TABLE alert_outbox ADD COLUMN claimed_by TEXT;")

    @staticmethod
    def _row_to_outbox_record(row: sqlite3.Row) -> AlertOutboxRecord:
        keys = row.keys()
        return AlertOutboxRecord(
            outbox_id=row["outbox_id"],
            run_id=row["run_id"],
            channel=row["channel"],
            severity_band=row["severity_band"],
            status=row["status"],
            payload_json=row["payload_json"],
            retry_count=row["retry_count"],
            max_retries=row["max_retries"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            error_message=row["error_message"],
            claim_token=row["claim_token"] if "claim_token" in keys else 0,
            claimed_by=row["claimed_by"] if "claimed_by" in keys else None,
        )

    def save_run(
        self,
        run: MonitoringRunResponse,
        outbox_records: Optional[List[AlertOutboxRecord]] = None,
    ) -> bool:
        with self._lock:
            if self.simulate_failure:
                raise RuntimeError(
                    "Simulated SQLite database failure: disk I/O error."
                )

            payload_json = run.model_dump_json()
            created_iso = run.created_at.isoformat()

            try:
                with self._conn:
                    # Insert main run record
                    self._conn.execute(
                        """
                        INSERT INTO monitoring_runs (
                            run_id, model_name, model_version, run_type, created_at,
                            sample_count, status, dataset_drift_detected,
                            severity_score, severity_band, raw_payload
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            run.run_id,
                            run.model_name,
                            run.model_version,
                            run.run_type,
                            created_iso,
                            run.sample_count,
                            run.status,
                            1 if run.dataset_drift_detected else 0,
                            run.severity.score,
                            run.severity.band,
                            payload_json,
                        ),
                    )

                    # Insert feature records for indexing
                    for f in run.features:
                        self._conn.execute(
                            """
                            INSERT INTO feature_drift_results (
                                run_id, feature_name, drift_detected, drift_contribution
                            ) VALUES (?, ?, ?, ?);
                            """,
                            (
                                run.run_id,
                                f.feature_name,
                                1 if f.drift_detected else 0,
                                f.drift_contribution,
                            ),
                        )

                    # Atomically insert outbox records within the SAME SQLite transaction
                    if outbox_records:
                        for record in outbox_records:
                            stat_val = (
                                record.status.value
                                if hasattr(record.status, "value")
                                else str(record.status)
                            )
                            self._conn.execute(
                                """
                                INSERT INTO alert_outbox (
                                    outbox_id, run_id, channel, severity_band, status,
                                    payload_json, retry_count, max_retries, created_at, updated_at, error_message,
                                    claim_token, claimed_by
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(outbox_id) DO UPDATE SET
                                    status = CASE 
                                        WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight') 
                                        THEN alert_outbox.status 
                                        ELSE excluded.status 
                                    END,
                                    retry_count = CASE 
                                        WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight') 
                                        THEN alert_outbox.retry_count 
                                        ELSE excluded.retry_count 
                                    END,
                                    updated_at = excluded.updated_at,
                                    error_message = CASE
                                        WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight')
                                        THEN alert_outbox.error_message
                                        ELSE excluded.error_message
                                    END,
                                    claim_token = CASE
                                        WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight')
                                        THEN alert_outbox.claim_token
                                        ELSE excluded.claim_token
                                    END,
                                    claimed_by = CASE
                                        WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight')
                                        THEN alert_outbox.claimed_by
                                        ELSE excluded.claimed_by
                                    END;
                                """,
                                (
                                    record.outbox_id,
                                    record.run_id,
                                    record.channel,
                                    record.severity_band,
                                    stat_val,
                                    record.payload_json,
                                    record.retry_count,
                                    record.max_retries,
                                    record.created_at.isoformat(),
                                    record.updated_at.isoformat(),
                                    record.error_message,
                                    record.claim_token,
                                    record.claimed_by,
                                ),
                            )
                return True
            except sqlite3.IntegrityError as exc:
                raise ValueError(
                    f"Duplicate run_id '{run.run_id}': record already exists in database."
                ) from exc

    def get_run(self, run_id: str) -> Optional[MonitoringRunResponse]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT raw_payload FROM monitoring_runs WHERE run_id = ?;",
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            data = json.loads(row["raw_payload"])
            return MonitoringRunResponse.model_validate(data)

    def list_runs(
        self, limit: int = 50, offset: int = 0
    ) -> List[MonitoringRunResponse]:
        with self._lock:
            cursor = self._conn.execute(
                """
                SELECT raw_payload FROM monitoring_runs
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?;
                """,
                (limit, offset),
            )
            rows = cursor.fetchall()
            results = []
            for row in rows:
                data = json.loads(row["raw_payload"])
                results.append(MonitoringRunResponse.model_validate(data))
            return results

    def count_runs(self) -> int:
        with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) AS total FROM monitoring_runs;")
            row = cursor.fetchone()
            return int(row["total"]) if row else 0

    def delete_run(self, run_id: str) -> bool:
        with self._lock:
            with self._conn:
                cursor = self._conn.execute(
                    "DELETE FROM monitoring_runs WHERE run_id = ?;",
                    (run_id,),
                )
                return cursor.rowcount > 0

    def save_alert_outbox(self, record: AlertOutboxRecord) -> bool:
        with self._lock:
            with self._conn:
                stat_val = record.status.value if hasattr(record.status, "value") else str(record.status)
                self._conn.execute(
                    """
                    INSERT INTO alert_outbox (
                        outbox_id, run_id, channel, severity_band, status,
                        payload_json, retry_count, max_retries, created_at, updated_at, error_message,
                        claim_token, claimed_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(outbox_id) DO UPDATE SET
                        status = CASE 
                            WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight') 
                            THEN alert_outbox.status 
                            ELSE excluded.status 
                        END,
                        retry_count = CASE 
                            WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight') 
                            THEN alert_outbox.retry_count 
                            ELSE excluded.retry_count 
                        END,
                        updated_at = excluded.updated_at,
                        error_message = CASE
                            WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight')
                            THEN alert_outbox.error_message
                            ELSE excluded.error_message
                        END,
                        claim_token = CASE
                            WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight')
                            THEN alert_outbox.claim_token
                            ELSE excluded.claim_token
                        END,
                        claimed_by = CASE
                            WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight')
                            THEN alert_outbox.claimed_by
                            ELSE excluded.claimed_by
                        END;
                    """,
                    (
                        record.outbox_id,
                        record.run_id,
                        record.channel,
                        record.severity_band,
                        stat_val,
                        record.payload_json,
                        record.retry_count,
                        record.max_retries,
                        record.created_at.isoformat(),
                        record.updated_at.isoformat(),
                        record.error_message,
                        record.claim_token,
                        record.claimed_by,
                    ),
                )
                return True

    def get_pending_alert_outbox(self, limit: int = 50) -> List[AlertOutboxRecord]:
        with self._lock:
            cursor = self._conn.execute(
                """
                SELECT * FROM alert_outbox
                WHERE status IN ('pending', 'retrying')
                  AND retry_count < max_retries
                ORDER BY created_at ASC
                LIMIT ?;
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            return [self._row_to_outbox_record(r) for r in rows]

    def claim_pending_alert_outbox(
        self, limit: int = 50, worker_id: str = "worker-default", lease_seconds: int = 300
    ) -> List[AlertOutboxRecord]:
        with self._lock:
            with self._conn:
                now = datetime.now(timezone.utc)
                now_iso = now.isoformat()
                cutoff_iso = (now - timedelta(seconds=lease_seconds)).isoformat()
                cursor = self._conn.execute(
                    """
                    SELECT outbox_id FROM alert_outbox
                    WHERE ((status IN ('pending', 'retrying')) OR (status = 'in_flight' AND updated_at < ?))
                      AND retry_count < max_retries
                    ORDER BY created_at ASC
                    LIMIT ?;
                    """,
                    (cutoff_iso, limit),
                )
                rows = cursor.fetchall()
                if not rows:
                    return []
                claimed_ids = [row["outbox_id"] for row in rows]
                placeholders = ",".join("?" * len(claimed_ids))
                claim_msg = f"Claimed by {worker_id}"
                self._conn.execute(
                    f"""
                    UPDATE alert_outbox
                    SET status = 'in_flight',
                        claim_token = claim_token + 1,
                        claimed_by = ?,
                        updated_at = ?,
                        error_message = ?
                    WHERE outbox_id IN ({placeholders});
                    """,
                    [worker_id, now_iso, claim_msg] + claimed_ids,
                )

                cursor = self._conn.execute(
                    f"""
                    SELECT * FROM alert_outbox
                    WHERE outbox_id IN ({placeholders});
                    """,
                    claimed_ids,
                )
                return [self._row_to_outbox_record(r) for r in cursor.fetchall()]

    def update_alert_outbox_status(
        self,
        outbox_id: str,
        status: str,
        error_message: Optional[str] = None,
        retry_count: Optional[int] = None,
        claim_token: Optional[int] = None,
    ) -> bool:
        with self._lock:
            with self._conn:
                now_iso = datetime.now(timezone.utc).isoformat()
                stat_val = status.value if hasattr(status, "value") else str(status)
                if retry_count is not None:
                    cursor = self._conn.execute(
                        """
                        UPDATE alert_outbox
                        SET status = ?, error_message = ?, retry_count = ?, updated_at = ?
                        WHERE outbox_id = ?
                          AND status NOT IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed')
                          AND (? IS NULL OR claim_token = ?);
                        """,
                        (stat_val, error_message, retry_count, now_iso, outbox_id, claim_token, claim_token),
                    )
                else:
                    cursor = self._conn.execute(
                        """
                        UPDATE alert_outbox
                        SET status = ?, error_message = ?, updated_at = ?
                        WHERE outbox_id = ?
                          AND status NOT IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed')
                          AND (? IS NULL OR claim_token = ?);
                        """,
                        (stat_val, error_message, now_iso, outbox_id, claim_token, claim_token),
                    )
                return cursor.rowcount > 0

    def reset_failed_outbox_record(self, outbox_id: str) -> bool:
        with self._lock:
            with self._conn:
                now_iso = datetime.now(timezone.utc).isoformat()
                cursor = self._conn.execute(
                    """
                    UPDATE alert_outbox
                    SET status = 'pending', retry_count = 0, error_message = 'Manually reset for replay', updated_at = ?
                    WHERE outbox_id = ? AND status = 'failed';
                    """,
                    (now_iso, outbox_id),
                )
                return cursor.rowcount > 0

    def get_alert_outbox_record(self, outbox_id: str) -> Optional[AlertOutboxRecord]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM alert_outbox WHERE outbox_id = ?;", (outbox_id,)
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_outbox_record(row)

    def close(self) -> None:
        """Close database connection."""
        with self._lock:
            self._conn.close()
        global _active_repository
        if _active_repository is self:
            _active_repository = None


# ──────────────────────────────────────────────────────────────────────────────
# REPOSITORY SINGLETON MANAGEMENT
# ──────────────────────────────────────────────────────────────────────────────

_active_repository: Optional[MonitoringRepository] = None


def get_monitoring_repository() -> MonitoringRepository:
    """Return active monitoring repository singleton."""
    global _active_repository
    if _active_repository is not None and isinstance(_active_repository, SQLiteMonitoringRepository):
        try:
            with _active_repository._lock:
                _active_repository._conn.execute("SELECT 1;")
        except (sqlite3.ProgrammingError, sqlite3.OperationalError):
            _active_repository = None

    if _active_repository is None:
        try:
            from backend.app.core.config import get_settings
            settings = get_settings()
            if settings.PERSISTENCE_TYPE == "sqlite":
                _active_repository = SQLiteMonitoringRepository(settings.SQLITE_DB_PATH)
            else:
                _active_repository = InMemoryMonitoringRepository()
        except Exception:
            _active_repository = InMemoryMonitoringRepository()
    return _active_repository


def set_monitoring_repository(repo: MonitoringRepository) -> None:
    """Set active monitoring repository implementation (testing hook)."""
    global _active_repository
    _active_repository = repo
