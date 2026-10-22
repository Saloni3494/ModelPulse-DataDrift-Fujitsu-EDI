
"""PostgreSQL persistence for ModelPulse monitoring runs and alert outbox."""

import json
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from backend.app.schemas.monitoring import MonitoringRunResponse
from backend.app.alerting.models import AlertOutboxRecord
from backend.app.db.repository import MonitoringRepository


class PostgreSQLMonitoringRepository(MonitoringRepository):
    """Persist monitoring runs, feature results, and alert outbox records."""

    def __init__(self, database_url: str):
        # psycopg expects postgresql://, not SQLAlchemy's asyncpg scheme.
        database_url = database_url.replace(
            "postgresql+asyncpg://", "postgresql://", 1
        )
        self.database_url = database_url
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self):
        return psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        )

    def _init_db(self) -> None:
        """Create the alert outbox table if it does not exist."""
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_outbox (
                    outbox_id VARCHAR(128) PRIMARY KEY,
                    run_id VARCHAR(64) NOT NULL
                        REFERENCES monitoring_runs(run_id) ON DELETE CASCADE,
                    channel VARCHAR(64) NOT NULL,
                    severity_band VARCHAR(16) NOT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    payload_json JSONB NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    error_message TEXT,
                    claim_token INTEGER NOT NULL DEFAULT 0,
                    claimed_by VARCHAR(128)
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_outbox_status
                ON alert_outbox (status, updated_at);
                """
            )

    def save_run(
        self,
        run: MonitoringRunResponse,
        outbox_records: Optional[List[AlertOutboxRecord]] = None,
    ) -> bool:
        """Save a run, its feature results, and outbox records atomically."""
        payload = run.model_dump(mode="json")
        payload_json = json.dumps(payload)

        prediction_drift = run.prediction_drift
        severity = run.severity

        try:
            with self._lock:
                with self._connect() as conn:
                    conn.execute(
                        """
                        INSERT INTO monitoring_runs (
                            run_id, model_name, model_version, run_type,
                            created_at, reference_start, reference_end,
                            reference_sample_count, current_start, current_end,
                            current_sample_count, sample_count, status,
                            dataset_drift_detected, prediction_drift_psi,
                            prediction_drift_detected, severity_score,
                            severity_band, severity_confidence,
                            component_d, component_p, component_q, component_m,
                            weights_used, top_contributors, recommendations,
                            raw_payload
                        ) VALUES (
                            %(run_id)s, %(model_name)s, %(model_version)s,
                            %(run_type)s, %(created_at)s, %(reference_start)s,
                            %(reference_end)s, %(reference_sample_count)s,
                            %(current_start)s, %(current_end)s, %(current_sample_count)s,
                            %(sample_count)s, %(status)s, %(dataset_drift_detected)s,
                            %(prediction_drift_psi)s, %(prediction_drift_detected)s,
                            %(severity_score)s, %(severity_band)s,
                            %(severity_confidence)s, %(component_d)s,
                            %(component_p)s, %(component_q)s, %(component_m)s,
                            %(weights_used)s::jsonb, %(top_contributors)s::jsonb,
                            %(recommendations)s::jsonb, %(raw_payload)s::jsonb
                        );
                        """,
                        {
                            "run_id": run.run_id,
                            "model_name": run.model_name,
                            "model_version": run.model_version,
                            "run_type": run.run_type,
                            "created_at": run.created_at,
                            "reference_start": run.reference_window.start,
                            "reference_end": run.reference_window.end,
                            "reference_sample_count": run.reference_window.sample_count,
                            "current_start": run.current_window.start,
                            "current_end": run.current_window.end,
                            "current_sample_count": run.current_window.sample_count,
                            "sample_count": run.sample_count,
                            "status": run.status,
                            "dataset_drift_detected": run.dataset_drift_detected,
                            "prediction_drift_psi": prediction_drift.value,
                            "prediction_drift_detected": prediction_drift.drifted,
                            "severity_score": severity.score,
                            "severity_band": severity.band,
                            "severity_confidence": severity.confidence,
                            "component_d": severity.components.get("D"),
                            "component_p": severity.components.get("P"),
                            "component_q": severity.components.get("Q"),
                            "component_m": severity.components.get("M"),
                            "weights_used": json.dumps(severity.weights_used),
                            "top_contributors": json.dumps(
                                [item.model_dump(mode="json") for item in run.top_contributors]
                            ),
                            "recommendations": json.dumps(run.recommendations),
                            "raw_payload": payload_json,
                        },
                    )

                    for feature in run.features:
                        test_details = [
                            test.model_dump(mode="json")
                            for test in feature.tests
                        ]
                        conn.execute(
                            """
                            INSERT INTO feature_drift_results (
                                run_id, feature_name, feature_type, votes,
                                votes_count, total_tests, drift_detected,
                                drift_contribution, unseen_categories,
                                reference_mean, current_mean, test_details
                            ) VALUES (
                                %(run_id)s, %(feature_name)s, %(feature_type)s,
                                %(votes)s, %(votes_count)s, %(total_tests)s,
                                %(drift_detected)s, %(drift_contribution)s,
                                %(unseen_categories)s::jsonb, %(reference_mean)s,
                                %(current_mean)s, %(test_details)s::jsonb
                            );
                            """,
                            {
                                "run_id": run.run_id,
                                "feature_name": feature.feature_name,
                                "feature_type": feature.feature_type,
                                "votes": feature.votes,
                                "votes_count": feature.votes_count,
                                "total_tests": feature.total_tests,
                                "drift_detected": feature.drift_detected,
                                "drift_contribution": feature.drift_contribution,
                                "unseen_categories": (
                                    json.dumps(feature.unseen_categories)
                                    if feature.unseen_categories is not None else None
                                ),
                                "reference_mean": feature.reference_mean,
                                "current_mean": feature.current_mean,
                                "test_details": json.dumps(test_details),
                            },
                        )

                    for record in outbox_records or []:
                        status = (
                            record.status.value
                            if hasattr(record.status, "value")
                            else str(record.status)
                        )
                        conn.execute(
                            """
                            INSERT INTO alert_outbox (
                                outbox_id, run_id, channel, severity_band, status,
                                payload_json, retry_count, max_retries, created_at,
                                updated_at, error_message, claim_token, claimed_by
                            ) VALUES (
                                %(outbox_id)s, %(run_id)s, %(channel)s,
                                %(severity_band)s, %(status)s, %(payload_json)s::jsonb,
                                %(retry_count)s, %(max_retries)s, %(created_at)s,
                                %(updated_at)s, %(error_message)s, %(claim_token)s,
                                %(claimed_by)s
                            );
                            """,
                            {
                                "outbox_id": record.outbox_id,
                                "run_id": record.run_id,
                                "channel": record.channel,
                                "severity_band": record.severity_band,
                                "status": status,
                                "payload_json": record.payload_json,
                                "retry_count": record.retry_count,
                                "max_retries": record.max_retries,
                                "created_at": record.created_at,
                                "updated_at": record.updated_at,
                                "error_message": record.error_message,
                                "claim_token": record.claim_token,
                                "claimed_by": record.claimed_by,
                            },
                        )
            return True

        except psycopg.errors.UniqueViolation as exc:
            raise ValueError(
                f"Duplicate run_id '{run.run_id}': record already exists in database."
            ) from exc
        except psycopg.Error as exc:
            raise RuntimeError(
                f"PostgreSQL monitoring persistence failed: {exc}"
            ) from exc

    def get_run(self, run_id: str) -> Optional[MonitoringRunResponse]:
        """Retrieve a monitoring run by ID."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT raw_payload FROM monitoring_runs WHERE run_id = %s",
                (run_id,),
            ).fetchone()

        if row is None:
            return None

        payload = row["raw_payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        return MonitoringRunResponse.model_validate(payload)

    def list_runs(
        self, limit: int = 50, offset: int = 0
    ) -> List[MonitoringRunResponse]:
        """List monitoring runs, newest first."""
        if limit < 0 or offset < 0:
            raise ValueError("limit and offset must be non-negative")

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT raw_payload
                FROM monitoring_runs
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            ).fetchall()

        results = []
        for row in rows:
            payload = row["raw_payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            results.append(MonitoringRunResponse.model_validate(payload))

        return results

    def count_runs(self) -> int:
        """Return the number of persisted monitoring runs."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM monitoring_runs"
            ).fetchone()

        return int(row["total"])

    def delete_run(self, run_id: str) -> bool:
        """Delete a run and its related feature/outbox records."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM monitoring_runs WHERE run_id = %s",
                (run_id,),
            )
            return cursor.rowcount > 0

    @staticmethod
    def _row_to_outbox_record(row: Dict[str, Any]) -> AlertOutboxRecord:
        payload = row["payload_json"]
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        return AlertOutboxRecord(
            outbox_id=row["outbox_id"],
            run_id=row["run_id"],
            channel=row["channel"],
            severity_band=row["severity_band"],
            status=row["status"],
            payload_json=payload,
            retry_count=row["retry_count"],
            max_retries=row["max_retries"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            error_message=row["error_message"],
            claim_token=row["claim_token"],
            claimed_by=row["claimed_by"],
        )

    def save_alert_outbox(self, record: AlertOutboxRecord) -> bool:
        """Insert an alert-outbox record if it does not already exist."""
        status = (
            record.status.value
            if hasattr(record.status, "value")
            else str(record.status)
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO alert_outbox (
                    outbox_id, run_id, channel, severity_band, status,
                    payload_json, retry_count, max_retries, created_at,
                    updated_at, error_message, claim_token, claimed_by
                ) VALUES (
                    %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (outbox_id) DO NOTHING
                """,
                (
                    record.outbox_id, record.run_id, record.channel,
                    record.severity_band, status, record.payload_json,
                    record.retry_count, record.max_retries, record.created_at,
                    record.updated_at, record.error_message, record.claim_token,
                    record.claimed_by,
                ),
            )
            return True

    def get_pending_alert_outbox(
        self, limit: int = 50
    ) -> List[AlertOutboxRecord]:
        """Return pending/retrying records eligible for delivery."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM alert_outbox
                WHERE status IN ('pending', 'retrying')
                  AND retry_count < max_retries
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()

        return [self._row_to_outbox_record(row) for row in rows]

    def claim_pending_alert_outbox(
        self,
        limit: int = 50,
        worker_id: str = "worker-default",
        lease_seconds: int = 300,
    ) -> List[AlertOutboxRecord]:
        """Claim eligible records with row locking and a lease."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=lease_seconds)

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT outbox_id
                FROM alert_outbox
                WHERE (
                    status IN ('pending', 'retrying')
                    OR (status = 'in_flight' AND updated_at < %s)
                )
                  AND retry_count < max_retries
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (cutoff, limit),
            ).fetchall()

            claimed_ids = [row["outbox_id"] for row in rows]
            if not claimed_ids:
                return []

            conn.execute(
                """
                UPDATE alert_outbox
                SET status = 'in_flight',
                    claim_token = claim_token + 1,
                    claimed_by = %s,
                    updated_at = %s,
                    error_message = %s
                WHERE outbox_id = ANY(%s)
                """,
                (worker_id, now, f"Claimed by {worker_id}", claimed_ids),
            )

            claimed = conn.execute(
                """
                SELECT * FROM alert_outbox
                WHERE outbox_id = ANY(%s)
                ORDER BY created_at ASC
                """,
                (claimed_ids,),
            ).fetchall()

        return [self._row_to_outbox_record(row) for row in claimed]

    def update_alert_outbox_status(
        self,
        outbox_id: str,
        status: str,
        error_message: Optional[str] = None,
        retry_count: Optional[int] = None,
        claim_token: Optional[int] = None,
    ) -> bool:
        """Update status without overwriting terminal states or stale claims."""
        status_value = (
            status.value if hasattr(status, "value") else str(status)
        )
        terminal = (
            "delivered", "suppressed_cooldown", "skipped_disabled", "failed"
        )

        with self._connect() as conn:
            if retry_count is not None:
                cursor = conn.execute(
                    """
                    UPDATE alert_outbox
                    SET status = %s, error_message = %s,
                        retry_count = %s, updated_at = %s
                    WHERE outbox_id = %s
                      AND status <> ALL(%s)
                      AND (%s::INTEGER IS NULL OR claim_token = %s)
                    """,
                    (
                        status_value, error_message, retry_count,
                        datetime.now(timezone.utc), outbox_id, list(terminal),
                        claim_token, claim_token,
                    ),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE alert_outbox
                    SET status = %s, error_message = %s, updated_at = %s
                    WHERE outbox_id = %s
                      AND status <> ALL(%s)
                      AND (%s::INTEGER IS NULL OR claim_token = %s)
                    """,
                    (
                        status_value, error_message, datetime.now(timezone.utc),
                        outbox_id, list(terminal), claim_token, claim_token,
                    ),
                )
            return cursor.rowcount > 0

    def reset_failed_outbox_record(self, outbox_id: str) -> bool:
        """Reset a failed record so it can be retried."""
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE alert_outbox
                SET status = 'pending', retry_count = 0,
                    error_message = 'Manually reset for replay',
                    updated_at = %s
                WHERE outbox_id = %s AND status = 'failed'
                """,
                (datetime.now(timezone.utc), outbox_id),
            )
            return cursor.rowcount > 0

    def get_alert_outbox_record(
        self, outbox_id: str
    ) -> Optional[AlertOutboxRecord]:
        """Retrieve one outbox record by ID."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM alert_outbox WHERE outbox_id = %s",
                (outbox_id,),
            ).fetchone()

        return self._row_to_outbox_record(row) if row else None

    def close(self) -> None:
        """Release repository singleton reference; connections are per-operation."""
        from backend.app.db import repository as repository_module

        if repository_module._active_repository is self:
            repository_module._active_repository = None
