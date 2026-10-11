"""ModelPulse Dead Letter Queue (DLQ) Service (Phase 5).
Safely captures and isolates unprocessable, invalid, or exhausted messages.
"""
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional
import uuid

from backend.app.schemas.events import DLQEvent
from backend.app.streaming.bus import MessageBus, get_message_bus

logger = logging.getLogger(__name__)


class DeadLetterQueueService:
    """Manages dead-letter queue routing and event preservation."""

    def __init__(
        self,
        dlq_topic: str = "events.dlq",
        message_bus: Optional[MessageBus] = None,
    ):
        self.dlq_topic = dlq_topic
        self.message_bus = message_bus or get_message_bus()
        self._sent_events: List[DLQEvent] = []
        self._failed_events: List[DLQEvent] = []

    async def route_to_dlq(
        self,
        original_payload: str,
        failure_category: str,
        diagnostic_message: str,
        event_id: Optional[str] = None,
        attempt_count: int = 1,
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> DLQEvent:
        """Construct DLQ event, publish to DLQ topic, and record in audit log."""
        dlq_event = DLQEvent(
            dlq_id=str(uuid.uuid4()),
            event_id=event_id,
            original_payload=original_payload,
            failure_category=failure_category,
            diagnostic_message=diagnostic_message,
            attempt_count=attempt_count,
            failure_timestamp=datetime.now(timezone.utc),
            headers=headers or {},
            correlation_id=correlation_id,
        )

        payload_json = dlq_event.model_dump_json()

        try:
            await self.message_bus.publish(
                topic=self.dlq_topic,
                value=payload_json,
                key=event_id,
                headers={"dlq_category": failure_category},
            )
            self._sent_events.append(dlq_event)
            logger.warning(
                f"[DLQ] Routed message to {self.dlq_topic}: dlq_id={dlq_event.dlq_id} "
                f"event_id={event_id} category={failure_category} reason={diagnostic_message}"
            )
        except Exception as exc:
            logger.error(
                f"[DLQ] CRITICAL: Failed to publish to DLQ topic {self.dlq_topic}: {exc}",
                exc_info=True,
            )
            self._failed_events.append(dlq_event)
            raise RuntimeError(
                f"DLQ publishing failed for event {event_id}: {exc}"
            ) from exc

        return dlq_event

    def get_sent_events(self) -> List[DLQEvent]:
        """Return history of DLQ events sent by this service instance."""
        return list(self._sent_events)

    def count_sent_events(self) -> int:
        """Return total count of DLQ events dispatched."""
        return len(self._sent_events)

    def clear(self) -> None:
        """Clear local DLQ audit history."""
        self._sent_events.clear()
