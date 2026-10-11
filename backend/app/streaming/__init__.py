"""ModelPulse Streaming Package (Phase 5).
Provides:
- MessageBus, InMemoryMessageBus, AIOKafkaMessageBus
- TransactionProducer, DeterministicReplayService
- DeadLetterQueueService
- MicroBatcher, MicroBatch
- StreamingConsumer
- StreamingMetrics, get_streaming_metrics
"""
from backend.app.streaming.bus import (
    BusMessage,
    MessageBus,
    InMemoryMessageBus,
    AIOKafkaMessageBus,
    get_message_bus,
    set_message_bus,
)
from backend.app.streaming.dlq import DeadLetterQueueService
from backend.app.streaming.producer import (
    TransactionProducer,
    DeterministicReplayService,
)
from backend.app.streaming.micro_batcher import (
    MicroBatch,
    MicroBatcher,
    align_to_window,
)
from backend.app.streaming.consumer import StreamingConsumer
from backend.app.streaming.metrics import (
    StreamingMetrics,
    get_streaming_metrics,
)

__all__ = [
    "BusMessage",
    "MessageBus",
    "InMemoryMessageBus",
    "AIOKafkaMessageBus",
    "get_message_bus",
    "set_message_bus",
    "DeadLetterQueueService",
    "TransactionProducer",
    "DeterministicReplayService",
    "MicroBatch",
    "MicroBatcher",
    "align_to_window",
    "StreamingConsumer",
    "StreamingMetrics",
    "get_streaming_metrics",
]
