"""ModelPulse Streaming Bus Abstraction & Implementations.
Provides:
- MessageBus (ABC): Common contract for streaming publish/subscribe operations.
- BusMessage: Standard message envelope with topic, partition, offset, key, value, and timestamp.
- InMemoryMessageBus: Deterministic, in-memory bus for offline testing and smoke environments.
- AIOKafkaMessageBus: Live asyncio Kafka producer/consumer adapter using aiokafka.
- get_message_bus(): Factory returning active bus singleton based on settings.
"""
from abc import ABC, abstractmethod
import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

logger = logging.getLogger(__name__)


class BusMessage:
    """Standard message envelope consumed from the message bus."""

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        value: str,
        key: Optional[str] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value
        self.key = key
        self.timestamp_ms = (
            timestamp_ms
            if timestamp_ms is not None
            else int(datetime.now(timezone.utc).timestamp() * 1000)
        )
        self.headers = headers or {}

    def __repr__(self) -> str:
        return f"<BusMessage topic={self.topic} p={self.partition} off={self.offset} key={self.key}>"


class MessageBus(ABC):
    """Abstract interface decoupling ModelPulse streaming services from broker implementations."""

    @abstractmethod
    async def publish(
        self,
        topic: str,
        value: str,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> int:
        """Publish message string to topic. Returns the assigned offset."""
        pass

    @abstractmethod
    async def consume(
        self,
        topic: str,
        group_id: str,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[BusMessage]:
        """Fetch uncommitted messages from topic for the specified consumer group."""
        pass

    @abstractmethod
    async def commit_offset(
        self,
        topic: str,
        group_id: str,
        partition: int,
        offset: int,
    ) -> None:
        """Commit highest processed offset for partition."""
        pass

    @abstractmethod
    async def get_committed_offset(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> int:
        """Return the highest committed offset for partition (or -1 if none)."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close bus connections and release resources."""
        pass


class InMemoryMessageBus(MessageBus):
    """Thread-safe, deterministic in-memory message bus for isolated unit testing.
    Supports multi-partition topologies without external network access.
    """

    def __init__(self):
        self._topics: Dict[str, Dict[int, List[BusMessage]]] = {}
        self._committed_offsets: Dict[str, Dict[str, Dict[int, int]]] = {}  # group -> topic -> partition -> offset
        self._consumer_positions: Dict[str, Dict[str, Dict[int, int]]] = {} # group -> topic -> partition -> current_read_index
        self._lock = asyncio.Lock()
        self.simulate_publish_failure: bool = False
        self.simulate_consume_failure: bool = False
        self.simulate_commit_failure: bool = False

    async def publish(
        self,
        topic: str,
        value: str,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> int:
        async with self._lock:
            if self.simulate_publish_failure:
                raise RuntimeError("Simulated broker outage: publish rejected.")

            if topic not in self._topics:
                self._topics[topic] = {}

            target_p = partition if partition is not None else 0
            if target_p not in self._topics[topic]:
                self._topics[topic][target_p] = []

            partition_messages = self._topics[topic][target_p]
            offset = len(partition_messages)

            msg = BusMessage(
                topic=topic,
                partition=target_p,
                offset=offset,
                value=value,
                key=key,
                headers=headers,
            )
            partition_messages.append(msg)
            return offset

    async def consume(
        self,
        topic: str,
        group_id: str,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[BusMessage]:
        async with self._lock:
            if self.simulate_consume_failure:
                raise RuntimeError("Simulated broker outage: consume timed out.")

            if topic not in self._topics:
                self._topics[topic] = {0: []}

            # Initialize consumer position structure
            if group_id not in self._consumer_positions:
                self._consumer_positions[group_id] = {}
            if topic not in self._consumer_positions[group_id]:
                self._consumer_positions[group_id][topic] = {}

            results: List[BusMessage] = []
            # Read across all partitions defined on this topic
            for partition in sorted(self._topics[topic].keys()):
                if len(results) >= max_records:
                    break
                remaining = max_records - len(results)
                committed = await self._get_committed_internal(topic, group_id, partition)
                current_pos = self._consumer_positions[group_id][topic].get(
                    partition, committed + 1
                )
                partition_messages = self._topics[topic].get(partition, [])
                unread = partition_messages[current_pos : current_pos + remaining]
                if unread:
                    new_pos = current_pos + len(unread)
                    self._consumer_positions[group_id][topic][partition] = new_pos
                    results.extend(unread)

            return results

    async def commit_offset(
        self,
        topic: str,
        group_id: str,
        partition: int,
        offset: int,
    ) -> None:
        async with self._lock:
            if self.simulate_commit_failure:
                raise RuntimeError("Simulated broker outage: offset commit failed.")

            if group_id not in self._committed_offsets:
                self._committed_offsets[group_id] = {}
            if topic not in self._committed_offsets[group_id]:
                self._committed_offsets[group_id][topic] = {}

            current_committed = self._committed_offsets[group_id][topic].get(partition, -1)
            # Only advance offset forward
            if offset > current_committed:
                self._committed_offsets[group_id][topic][partition] = offset

    async def get_committed_offset(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> int:
        async with self._lock:
            return await self._get_committed_internal(topic, group_id, partition)

    async def _get_committed_internal(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> int:
        return (
            self._committed_offsets.get(group_id, {})
            .get(topic, {})
            .get(partition, -1)
        )

    async def reset_consumer_position(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
        position: Optional[int] = None,
    ) -> None:
        """Reset consumer position to committed offset or explicit position."""
        async with self._lock:
            if position is None:
                committed = await self._get_committed_internal(topic, group_id, partition)
                position = committed + 1
            if group_id not in self._consumer_positions:
                self._consumer_positions[group_id] = {}
            if topic not in self._consumer_positions[group_id]:
                self._consumer_positions[group_id][topic] = {}
            self._consumer_positions[group_id][topic][partition] = position

    def get_topic_messages(
        self, topic: str, partition: Optional[int] = None
    ) -> List[BusMessage]:
        """Return all messages currently published to a topic (testing hook).
        If partition is None, returns all messages across all partitions.
        """
        if partition is not None:
            return list(self._topics.get(topic, {}).get(partition, []))
        all_msgs: List[BusMessage] = []
        for p in sorted(self._topics.get(topic, {}).keys()):
            all_msgs.extend(self._topics.get(topic, {}).get(p, []))
        return all_msgs

    def clear(self) -> None:
        """Reset all topic messages and consumer positions."""
        self._topics.clear()
        self._committed_offsets.clear()
        self._consumer_positions.clear()
        self.simulate_publish_failure = False
        self.simulate_consume_failure = False
        self.simulate_commit_failure = False

    async def close(self) -> None:
        pass


class AIOKafkaMessageBus(MessageBus):
    """Production asyncio Kafka client adapter using aiokafka."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        auto_offset_reset: str = "earliest",
        acks: Optional[Any] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset
        if acks is None:
            try:
                from backend.app.core.config import get_settings
                acks = get_settings().KAFKA_PRODUCER_ACKS
            except Exception:
                acks = "all"
        self.acks = acks
        self._producer: Optional[Any] = None
        self._consumers: Dict[Tuple[str, str], Any] = {}  # (topic, group_id) -> consumer
        self._lock = asyncio.Lock()

    async def _get_producer(self) -> Any:
        if self._producer is None:
            from aiokafka import AIOKafkaProducer
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks=self.acks,
            )
            await self._producer.start()
        return self._producer

    async def _get_consumer(self, topic: str, group_id: str) -> Any:
        key = (topic, group_id)
        if key not in self._consumers:
            from aiokafka import AIOKafkaConsumer
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=False,  # Enforce manual offset commit
            )
            await consumer.start()
            self._consumers[key] = consumer
        return self._consumers[key]

    async def publish(
        self,
        topic: str,
        value: str,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> int:
        producer = await self._get_producer()
        val_bytes = value.encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None
        kafka_headers = (
            [(k, v.encode("utf-8")) for k, v in headers.items()] if headers else None
        )
        record_metadata = await producer.send_and_wait(
            topic=topic,
            value=val_bytes,
            key=key_bytes,
            headers=kafka_headers,
            partition=partition,
        )
        return record_metadata.offset

    async def consume(
        self,
        topic: str,
        group_id: str,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[BusMessage]:
        consumer = await self._get_consumer(topic, group_id)
        data = await consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        results: List[BusMessage] = []
        for tp, messages in data.items():
            for m in messages:
                val_str = m.value.decode("utf-8") if isinstance(m.value, bytes) else str(m.value)
                key_str = m.key.decode("utf-8") if (m.key and isinstance(m.key, bytes)) else (m.key or None)
                headers_dict = {}
                if m.headers:
                    for hk, hv in m.headers:
                        headers_dict[hk] = hv.decode("utf-8") if isinstance(hv, bytes) else str(hv)
                results.append(
                    BusMessage(
                        topic=m.topic,
                        partition=m.partition,
                        offset=m.offset,
                        value=val_str,
                        key=key_str,
                        timestamp_ms=m.timestamp,
                        headers=headers_dict,
                    )
                )
        return results

    async def commit_offset(
        self,
        topic: str,
        group_id: str,
        partition: int,
        offset: int,
    ) -> None:
        from aiokafka.structs import TopicPartition, OffsetAndMetadata
        consumer = await self._get_consumer(topic, group_id)
        tp = TopicPartition(topic, partition)
        # Commit offset + 1 as per Kafka convention (next expected fetch offset)
        await consumer.commit({tp: OffsetAndMetadata(offset + 1, "")})

    async def get_committed_offset(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> int:
        from aiokafka.structs import TopicPartition
        consumer = await self._get_consumer(topic, group_id)
        tp = TopicPartition(topic, partition)
        comm = await consumer.committed(tp)
        return (comm - 1) if (comm is not None and comm > 0) else -1

    async def close(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None
        for consumer in self._consumers.values():
            await consumer.stop()
        self._consumers.clear()


# ──────────────────────────────────────────────────────────────────────────────
# SINGLETON MANAGEMENT
# ──────────────────────────────────────────────────────────────────────────────

_active_message_bus: Optional[MessageBus] = None


def get_message_bus() -> MessageBus:
    """Return active message bus singleton based on settings."""
    global _active_message_bus
    if _active_message_bus is None:
        from backend.app.core.config import get_settings
        settings = get_settings()
        if settings.KAFKA_MODE == "broker":
            _active_message_bus = AIOKafkaMessageBus(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
            )
        else:
            _active_message_bus = InMemoryMessageBus()
    return _active_message_bus


def set_message_bus(bus: MessageBus) -> None:
    """Set active message bus implementation (testing hook)."""
    global _active_message_bus
    _active_message_bus = bus
