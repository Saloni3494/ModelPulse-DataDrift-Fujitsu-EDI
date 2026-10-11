"""ModelPulse Streaming Telemetry & Metrics (Phase 5).
Tracks operational counters, processing latencies, and reliability indicators.
"""
from datetime import datetime, timezone
import threading
from typing import Any, Dict, Optional


class StreamingMetrics:
    """Thread-safe operational counters and telemetry for streaming pipeline."""

    def __init__(self):
        self._lock = threading.Lock()
        self.events_consumed: int = 0
        self.events_validated: int = 0
        self.events_quarantined: int = 0
        self.events_duplicated: int = 0
        self.events_dlq: int = 0
        self.events_retried: int = 0
        self.batches_completed: int = 0
        self.batches_failed: int = 0
        self.batches_empty: int = 0
        self.monitoring_runs_triggered: int = 0
        self.monitoring_runs_succeeded: int = 0
        self.monitoring_runs_failed: int = 0
        self.persistence_errors: int = 0
        self.retry_exhaustions: int = 0
        self.total_batch_processing_time_ms: float = 0.0
        self.last_event_time: Optional[datetime] = None

    def record_event_consumed(self) -> None:
        with self._lock:
            self.events_consumed += 1
            self.last_event_time = datetime.now(timezone.utc)

    def record_event_validated(self) -> None:
        with self._lock:
            self.events_validated += 1

    def record_event_quarantined(self) -> None:
        with self._lock:
            self.events_quarantined += 1

    def record_event_duplicated(self) -> None:
        with self._lock:
            self.events_duplicated += 1

    def record_event_dlq(self) -> None:
        with self._lock:
            self.events_dlq += 1

    def record_event_retried(self) -> None:
        with self._lock:
            self.events_retried += 1

    def record_batch_completed(self, duration_ms: float, status: str = "completed") -> None:
        with self._lock:
            self.batches_completed += 1
            self.total_batch_processing_time_ms += duration_ms
            if status == "empty":
                self.batches_empty += 1
            else:
                self.monitoring_runs_triggered += 1
                self.monitoring_runs_succeeded += 1

    def record_batch_failed(self) -> None:
        with self._lock:
            self.batches_failed += 1
            self.monitoring_runs_triggered += 1
            self.monitoring_runs_failed += 1

    def record_persistence_error(self) -> None:
        with self._lock:
            self.persistence_errors += 1

    def record_retry_exhausted(self) -> None:
        with self._lock:
            self.retry_exhaustions += 1

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            avg_batch_ms = (
                (self.total_batch_processing_time_ms / self.batches_completed)
                if self.batches_completed > 0
                else 0.0
            )
            return {
                "events_consumed": self.events_consumed,
                "events_validated": self.events_validated,
                "events_quarantined": self.events_quarantined,
                "events_duplicated": self.events_duplicated,
                "events_dlq": self.events_dlq,
                "events_retried": self.events_retried,
                "batches_completed": self.batches_completed,
                "batches_failed": self.batches_failed,
                "batches_empty": self.batches_empty,
                "monitoring_runs_triggered": self.monitoring_runs_triggered,
                "monitoring_runs_succeeded": self.monitoring_runs_succeeded,
                "monitoring_runs_failed": self.monitoring_runs_failed,
                "persistence_errors": self.persistence_errors,
                "retry_exhaustions": self.retry_exhaustions,
                "avg_batch_processing_time_ms": round(avg_batch_ms, 2),
                "last_event_time": (
                    self.last_event_time.isoformat() if self.last_event_time else None
                ),
            }

    def get_reconciliation_table(self) -> Dict[str, Any]:
        """Produce independently reconciled event accounting audit table.
        Guarantees:
        - Mutually exclusive terminal states: valid, quarantined, dlq, failed_uncommitted
        - Retries and duplicate deliveries are observed without inflating unique counts.
        - Exact Conservation: total_consumed == unique_valid + unique_quarantined + unique_dlq + unique_failed + duplicate_deliveries
        """
        with self._lock:
            terminal_unique = (
                self.events_validated
                + self.events_quarantined
                + self.events_dlq
                + self.persistence_errors
            )
            total_accounted = terminal_unique + self.events_duplicated
            discrepancy = self.events_consumed - total_accounted
            return {
                "total_consumed_arrivals": self.events_consumed,
                "terminal_unique_valid": self.events_validated,
                "terminal_unique_quarantined": self.events_quarantined,
                "terminal_unique_dlq": self.events_dlq,
                "terminal_unique_failed_uncommitted": self.persistence_errors,
                "observed_duplicate_deliveries": self.events_duplicated,
                "observed_retry_attempts": self.events_retried,
                "total_accounted": total_accounted,
                "discrepancy": discrepancy,
                "is_balanced": (discrepancy == 0),
            }

    def reset(self) -> None:
        with self._lock:
            self.events_consumed = 0
            self.events_validated = 0
            self.events_quarantined = 0
            self.events_duplicated = 0
            self.events_dlq = 0
            self.events_retried = 0
            self.batches_completed = 0
            self.batches_failed = 0
            self.batches_empty = 0
            self.monitoring_runs_triggered = 0
            self.monitoring_runs_succeeded = 0
            self.monitoring_runs_failed = 0
            self.persistence_errors = 0
            self.retry_exhaustions = 0
            self.total_batch_processing_time_ms = 0.0
            self.last_event_time = None


# Global streaming metrics singleton
_global_metrics = StreamingMetrics()


def get_streaming_metrics() -> StreamingMetrics:
    """Return global streaming metrics instance."""
    return _global_metrics
