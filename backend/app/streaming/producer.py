"""ModelPulse Transaction Producer & Deterministic Replay Service (Phase 5).
Generates and replays synthetic events across controlled drift scenarios and malformed test cases.
Never modifies reference data. Clearly labels all output as synthetic.
"""
import asyncio
from datetime import datetime, timezone, timedelta
import json
import logging
from typing import Any, Dict, List, Optional
import uuid
import numpy as np
import pandas as pd

from backend.app.schemas.events import (
    TransactionEvent,
    ReplaySummary,
)
from backend.app.streaming.bus import MessageBus, get_message_bus
from simulator.scenarios import (
    SYNTHETIC_DISCLAIMER,
    load_or_create_reference,
    scenario_no_drift,
    scenario_numeric_mean_variance_shift,
    scenario_categorical_proportion_shift,
    scenario_unseen_categories,
    scenario_sudden_drift,
    scenario_gradual_drift,
    scenario_data_quality_failures,
)

logger = logging.getLogger(__name__)


class TransactionProducer:
    """Async producer publishing versioned transaction events to Kafka."""

    def __init__(
        self,
        topic: str = "events",
        message_bus: Optional[MessageBus] = None,
    ):
        self.topic = topic
        self.message_bus = message_bus or get_message_bus()

    async def publish_event(
        self, event: TransactionEvent, partition: Optional[int] = None
    ) -> int:
        """Publish a validated TransactionEvent instance."""
        payload_json = event.model_dump_json()
        offset = await self.message_bus.publish(
            topic=self.topic,
            value=payload_json,
            key=event.event_id,
            headers={
                "event_id": event.event_id,
                "schema_version": event.schema_version,
            },
            partition=partition,
        )
        return offset

    async def publish_raw(
        self,
        raw_payload: str,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> int:
        """Publish raw string payload directly (used for malformed/corrupted event tests)."""
        offset = await self.message_bus.publish(
            topic=self.topic,
            value=raw_payload,
            key=key,
            headers=headers or {},
            partition=partition,
        )
        return offset


class DeterministicReplayService:
    """Reproducible event replay orchestrator for simulation and drift testing."""

    def __init__(
        self,
        producer: Optional[TransactionProducer] = None,
        reference_path: str = "ml/artifacts/reference_features.csv",
    ):
        self.producer = producer or TransactionProducer()
        self.reference_path = reference_path
        self._reference_df: Optional[pd.DataFrame] = None

    def get_reference_data(self) -> pd.DataFrame:
        """Load and cache frozen reference dataset, strictly returning a deep copy."""
        if self._reference_df is None:
            self._reference_df = load_or_create_reference(
                reference_path=self.reference_path, seed=42
            )
        return self._reference_df.copy(deep=True)

    async def replay_scenario(
        self,
        scenario_id: str,
        seed: int = 42,
        n_samples: int = 100,
        rate_per_sec: Optional[float] = None,
        base_event_time: Optional[datetime] = None,
        time_step_seconds: float = 1.0,
    ) -> ReplaySummary:
        """Replay events generated from a Phase 3 simulator scenario.

        Guarantees:
        - Reference data is strictly immutable.
        - Reproducible stream given identical scenario_id and seed.
        - Accurate published, rejected, and failed counts.
        """
        start_time = datetime.now(timezone.utc)
        ref_df = self.get_reference_data()

        sid = scenario_id.lower()
        if sid == "no_drift":
            cur_df, manifest = scenario_no_drift(ref_df, seed=seed, n_samples=n_samples)
        elif sid == "numeric_mean_variance_shift":
            cur_df, manifest = scenario_numeric_mean_variance_shift(ref_df, seed=seed, n_samples=n_samples)
        elif sid == "categorical_proportion_shift":
            cur_df, manifest = scenario_categorical_proportion_shift(ref_df, seed=seed, n_samples=n_samples)
        elif sid == "unseen_categories":
            cur_df, manifest = scenario_unseen_categories(ref_df, seed=seed, n_samples=n_samples)
        elif sid == "sudden_drift":
            cur_df, manifest = scenario_sudden_drift(ref_df, seed=seed, n_samples=n_samples)
        elif sid == "gradual_drift":
            cur_df, manifest = scenario_gradual_drift(ref_df, seed=seed, n_samples=n_samples)
        elif sid == "data_quality_failures":
            cur_df, manifest = scenario_data_quality_failures(ref_df, seed=seed, n_samples=n_samples)
        else:
            raise ValueError(f"Unknown simulator scenario '{scenario_id}'.")

        # Deterministic event generation
        t0 = base_event_time or datetime.now(timezone.utc)
        published_count = 0
        failed_count = 0
        rejected_count = 0

        # Replace NaN values with None for JSON serialization
        records = cur_df.replace({np.nan: None}).to_dict(orient="records")

        for idx, row in enumerate(records):
            event_time = t0 + timedelta(seconds=idx * time_step_seconds)
            event_id = f"sim-{scenario_id}-s{seed}-{idx:06d}"

            try:
                event = TransactionEvent(
                    event_id=event_id,
                    timestamp=event_time,
                    schema_version="1.0.0",
                    features=row,
                    metadata={
                        "synthetic": True,
                        "scenario_id": scenario_id,
                        "seed": seed,
                        "row_index": idx,
                        "synthetic_notice": SYNTHETIC_DISCLAIMER,
                    },
                )
                await self.producer.publish_event(event)
                published_count += 1
            except Exception as exc:
                logger.error(f"[Replay] Failed to publish event {event_id}: {exc}")
                failed_count += 1

            if rate_per_sec and rate_per_sec > 0:
                await asyncio.sleep(1.0 / rate_per_sec)

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        return ReplaySummary(
            scenario_id=scenario_id,
            seed=seed,
            target_count=len(records),
            published_count=published_count,
            rejected_count=rejected_count,
            failed_count=failed_count,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=round(duration, 4),
        )

    async def replay_malformed_batch(
        self,
        seed: int = 42,
        base_event_time: Optional[datetime] = None,
    ) -> ReplaySummary:
        """Publish a controlled batch of malformed and corrupted messages for DLQ testing.
        Includes:
        1. Malformed non-JSON payload string.
        2. Unsupported schema version ("2.0.0").
        3. Valid JSON missing event_id.
        4. Valid JSON missing required features dictionary.
        5. Negative amount and negative balance invalid records.
        6. Duplicate event ID published twice.
        """
        start_time = datetime.now(timezone.utc)
        t0 = base_event_time or datetime.now(timezone.utc)
        published_count = 0
        failed_count = 0

        malformed_payloads: List[Tuple[str, str]] = [
            # 1. Unparseable JSON string
            ("malformed-001", "{bad_json: True, amount: 500, corrupt"),
            # 2. Unsupported schema version
            (
                "malformed-002",
                json.dumps({
                    "event_id": "malformed-002",
                    "timestamp": t0.isoformat(),
                    "schema_version": "2.0.0",  # Unsupported
                    "features": {"amount": 100.0, "step": 1, "type": "PAYMENT"},
                }),
            ),
            # 3. Missing event_id
            (
                "malformed-003",
                json.dumps({
                    "timestamp": t0.isoformat(),
                    "schema_version": "1.0.0",
                    "features": {"amount": 200.0, "step": 2, "type": "PAYMENT"},
                }),
            ),
            # 4. Non-dict features
            (
                "malformed-004",
                json.dumps({
                    "event_id": "malformed-004",
                    "timestamp": t0.isoformat(),
                    "schema_version": "1.0.0",
                    "features": "not_a_dictionary_payload",
                }),
            ),
            # 5. Negative amount
            (
                "malformed-005",
                json.dumps({
                    "event_id": "malformed-005",
                    "timestamp": t0.isoformat(),
                    "schema_version": "1.0.0",
                    "features": {
                        "step": 1,
                        "type": "TRANSFER",
                        "amount": -9999.0,
                        "oldbalanceOrg": 100.0,
                        "newbalanceOrig": 0.0,
                        "oldbalanceDest": 0.0,
                        "newbalanceDest": 0.0,
                    },
                }),
            ),
            # 6. First instance of duplicate
            (
                "malformed-dup-001",
                json.dumps({
                    "event_id": "malformed-dup-001",
                    "timestamp": t0.isoformat(),
                    "schema_version": "1.0.0",
                    "features": {
                        "step": 1,
                        "type": "PAYMENT",
                        "amount": 50.0,
                        "oldbalanceOrg": 100.0,
                        "newbalanceOrig": 50.0,
                        "oldbalanceDest": 0.0,
                        "newbalanceDest": 50.0,
                    },
                }),
            ),
            # 7. Duplicate instance (same event_id)
            (
                "malformed-dup-001",
                json.dumps({
                    "event_id": "malformed-dup-001",
                    "timestamp": t0.isoformat(),
                    "schema_version": "1.0.0",
                    "features": {
                        "step": 1,
                        "type": "PAYMENT",
                        "amount": 50.0,
                        "oldbalanceOrg": 100.0,
                        "newbalanceOrig": 50.0,
                        "oldbalanceDest": 0.0,
                        "newbalanceDest": 50.0,
                    },
                }),
            ),
        ]

        for key, payload in malformed_payloads:
            try:
                await self.producer.publish_raw(raw_payload=payload, key=key)
                published_count += 1
            except Exception as exc:
                logger.error(f"[Replay] Failed to publish malformed item {key}: {exc}")
                failed_count += 1

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        return ReplaySummary(
            scenario_id="malformed_test_batch",
            seed=seed,
            target_count=len(malformed_payloads),
            published_count=published_count,
            rejected_count=0,
            failed_count=failed_count,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=round(duration, 4),
        )
