
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from backend.app.core.config import get_settings


class PredictionRepository:
    """Persistent storage for prediction events using SQLite or PostgreSQL."""

    def __init__(
        self,
        db_path: str = "data/modelpulse.db",
        database_url: str | None = None,
    ):
        self.db_path = db_path
        settings = get_settings()
        self.database_url = (
                database_url
                or os.getenv("PREDICTION_DATABASE_URL")
                or settings.DATABASE_URL
            )

        if self.database_url:
            self.database_url = self.database_url.replace(
                "postgresql+asyncpg://",
                "postgresql://",
                1,
            )
            self._initialize_postgres()
        else:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self._initialize_sqlite()

    def _connect(self):
        if self.database_url:
            return psycopg.connect(
                self.database_url,
                row_factory=dict_row,
            )

        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize_sqlite(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS prediction_events (
                    event_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    model_name TEXT,
                    model_version TEXT,
                    prediction TEXT,
                    score REAL,
                    validation_status TEXT NOT NULL,
                    error_reason TEXT,
                    features_json TEXT NOT NULL
                )
            """)

    def _initialize_postgres(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS prediction_events (
                    event_id VARCHAR(128) PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    model_name VARCHAR(64),
                    model_version VARCHAR(32),
                    prediction TEXT,
                    score DOUBLE PRECISION,
                    validation_status VARCHAR(32) NOT NULL,
                    error_reason TEXT,
                    features_json JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)

    def save_prediction(self, event: dict[str, Any]) -> bool:
        """Insert an event once. Return False if its ID already exists."""
        event_id = event.get("event_id")
        if not event_id:
            raise ValueError("event_id is required")

        features_json = json.dumps(event.get("features", {}), default=str)

        values = (
            str(event_id),
            str(event.get("timestamp", "")),
            event.get("model_name"),
            event.get("model_version"),
            (
                str(event["prediction"])
                if event.get("prediction") is not None
                else None
            ),
            event.get("score"),
            str(event.get("validation_status", "valid")),
            event.get("error_reason"),
            features_json,
        )

        with self._connect() as conn:
            if self.database_url:
                cursor = conn.execute("""
                    INSERT INTO prediction_events (
                        event_id, timestamp, model_name, model_version,
                        prediction, score, validation_status,
                        error_reason, features_json
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT (event_id) DO NOTHING
                """, values)
            else:
                cursor = conn.execute("""
                    INSERT OR IGNORE INTO prediction_events (
                        event_id, timestamp, model_name, model_version,
                        prediction, score, validation_status,
                        error_reason, features_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, values)

            return cursor.rowcount == 1

    def get_prediction(self, event_id: str):
        with self._connect() as conn:
            placeholder = "%s" if self.database_url else "?"
            row = conn.execute(
                f"SELECT * FROM prediction_events "
                f"WHERE event_id = {placeholder}",
                (event_id,),
            ).fetchone()

        if row is None:
            return None

        result = dict(row)
        features = result.pop("features_json")

        if isinstance(features, str):
            features = json.loads(features)

        result["features"] = features
        return result

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM prediction_events"
            ).fetchone()

        return row["total"] if self.database_url else row["total"]
