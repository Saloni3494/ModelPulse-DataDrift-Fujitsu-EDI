"""ModelPulse Event Contracts & Streaming Schemas (Phase 5).
Defines versioned schemas for:
- TransactionEvent: Versioned streaming transaction event payload.
- DLQEvent: Dead-letter queue record capturing unprocessable/invalid messages.
- MicroBatchMetadata: Audit and telemetry metadata for 5-minute micro-batches.
- ReplaySummary: Summary report for deterministic synthetic event replay.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import uuid
from pydantic import BaseModel, Field, field_validator

SUPPORTED_SCHEMA_VERSIONS = {"1.0.0"}


class TransactionEvent(BaseModel):
    """Versioned transaction event published to Kafka."""

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Stable unique transaction event ID",
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event creation timestamp in UTC",
    )
    schema_version: str = Field(
        default="1.0.0",
        description="Event contract schema version (currently '1.0.0')",
    )
    features: Dict[str, Any] = Field(
        ...,
        description="Transaction feature payload (step, type, amount, balances, etc.)",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Operational metadata: correlation_id, synthetic flag, scenario_id",
    )

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        if not v or not isinstance(v, str) or not v.strip():
            raise ValueError("event_id must be a non-empty string.")
        return v.strip()

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"Unsupported schema version '{v}'. Supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)}"
            )
        return v


class DLQEvent(BaseModel):
    """Dead Letter Queue record capturing unprocessable or permanently failed events."""

    dlq_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique dead-letter record ID",
    )
    event_id: Optional[str] = Field(
        default=None,
        description="Original event ID if recoverable from payload",
    )
    original_payload: str = Field(
        ...,
        description="Exact raw message payload string as received from Kafka",
    )
    failure_category: str = Field(
        ...,
        description="'malformed_json', 'unsupported_schema_version', 'validation_error', 'processing_error', 'retry_exhausted'",
    )
    diagnostic_message: str = Field(
        ...,
        description="Human-readable exception details or validation failure reason",
    )
    attempt_count: int = Field(
        default=1,
        ge=1,
        description="Delivery or processing attempt count before DLQ routing",
    )
    failure_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when event was routed to DLQ (UTC)",
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description="Message transport headers",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="Tracing correlation ID for end-to-end audit",
    )


class MicroBatchMetadata(BaseModel):
    """Audit metadata and telemetry for a five-minute micro-batch."""

    batch_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique micro-batch run ID",
    )
    window_start: datetime = Field(
        ...,
        description="Event-time lower bound for the micro-batch window (inclusive)",
    )
    window_end: datetime = Field(
        ...,
        description="Event-time upper bound for the micro-batch window (exclusive)",
    )
    processing_time_ms: float = Field(
        default=0.0,
        ge=0.0,
        description="Execution time of the batch monitoring cycle in milliseconds",
    )
    source_offsets: Dict[str, int] = Field(
        default_factory=dict,
        description="Mapping of partition (as string key) to highest consumed offset",
    )
    tracked_offsets: List[Tuple[int, int]] = Field(
        default_factory=list,
        description="List of (partition, offset) pairs tracked in this micro-batch",
    )
    received_count: int = Field(
        default=0, ge=0, description="Total events ingested from Kafka partition(s)"
    )
    valid_count: int = Field(
        default=0, ge=0, description="Events passing schema and feature validation"
    )
    quarantined_count: int = Field(
        default=0, ge=0, description="Events with invalid feature values quarantined"
    )
    duplicate_count: int = Field(
        default=0, ge=0, description="Events detected as duplicates and ignored"
    )
    dlq_count: int = Field(
        default=0, ge=0, description="Malformed events routed to DLQ"
    )
    failed_count: int = Field(
        default=0, ge=0, description="Events permanently failing after retries"
    )
    monitoring_run_id: Optional[str] = Field(
        default=None, description="UUID of persisted MonitoringRunResponse"
    )
    status: str = Field(
        default="completed",
        description="'completed', 'empty', 'all_quarantined', 'partial_failure'",
    )


class ReplaySummary(BaseModel):
    """Execution report for deterministic producer replay."""

    replay_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    scenario_id: Optional[str] = None
    seed: int = 42
    target_count: int = 0
    published_count: int = 0
    rejected_count: int = 0
    failed_count: int = 0
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    synthetic_notice: str = (
        "SYNTHETIC EVENT DATASET: Replayed events are synthetically generated for testing "
        "and drift monitoring verification. Never treated as real production PaySim traffic."
    )
