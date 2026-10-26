import asyncio
import logging
import signal
import threading
from datetime import UTC, datetime

import config
from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class EventProducer:
    """One per process. Safe config, background polling, three ways to send (Lesson 05)."""

    def __init__(self, delivery_timeout_ms: int = 10_000):
        self._producer = Producer(
            {
                "bootstrap.servers": config.BOOTSTRAP_SERVERS,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
                "compression.type": "zstd",
                # Report a failure after this long instead of retrying for the default 5 minutes
                "delivery.timeout.ms": delivery_timeout_ms,
            }
        )
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, daemon=True)
        self._poller.start()

    def _poll_loop(self):
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def _produce(self, topic, key, value, headers, on_delivery):
        if isinstance(value, BaseModel):
            value = value.model_dump_json().encode()
        if isinstance(key, str):
            key = key.encode()
        # BufferError (local queue full) is NOT caught: the caller decides (the API answers 503)
        self._producer.produce(topic, key=key, value=value, headers=headers, on_delivery=on_delivery)

    def send_sync(self, topic, key, value, headers=None, timeout: float = 15.0) -> tuple[int, int]:
        """Blocks until the broker confirmed the write. Returns (partition, offset) or raises."""
        done = threading.Event()
        result = {}

        def on_delivery(err, msg):
            result["err"], result["msg"] = err, msg
            done.set()

        self._produce(topic, key, value, headers, on_delivery)
        if not done.wait(timeout):
            raise TimeoutError(f"no delivery report from Kafka within {timeout}s")
        if result["err"] is not None:
            raise KafkaException(result["err"])
        return result["msg"].partition(), result["msg"].offset()

    async def send_async(self, topic, key, value, headers=None, timeout: float = 15.0) -> tuple[int, int]:
        """Like send_sync, but awaitable: the event loop keeps serving other requests meanwhile."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def resolve(err, msg):
            if future.done():  # we already gave up waiting (timeout)
                return
            if err is not None:
                future.set_exception(KafkaException(err))
            else:
                future.set_result((msg.partition(), msg.offset()))

        # The callback runs in the poll thread: hand the result over to the event loop's thread
        self._produce(topic, key, value, headers, lambda err, msg: loop.call_soon_threadsafe(resolve, err, msg))
        return await asyncio.wait_for(future, timeout)

    def close(self, timeout: float = 10) -> int:
        self._stop.set()
        self._poller.join()
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.error("%d messages NOT delivered at shutdown", remaining)
        return remaining


def make_consumer(group_id: str, topics: list[str]) -> Consumer:
    """At-least-once consumer: you call store_offsets(msg) once a message is handled (Lessons 03-04)."""
    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "session.timeout.ms": 10_000,
        }
    )

    def on_assign(c, partitions):
        logger.info("ASSIGNED %s", [f"{p.topic}[{p.partition}]" for p in partitions])

    def on_revoke(c, partitions):
        logger.info("REVOKED %s", [f"{p.topic}[{p.partition}]" for p in partitions])

    consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)
    return consumer


def send_to_dlq(producer: EventProducer, msg, dlq_topic: str, group: str, error_type: str, error: Exception):
    """Original bytes + why/where in headers (Lesson 06). Raises if the DLQ write fails."""
    headers = (msg.headers() or []) + [
        ("dlq.error.type", error_type),
        ("dlq.error.message", str(error)[:1000]),
        ("dlq.original.topic", msg.topic()),
        ("dlq.original.partition", str(msg.partition())),
        ("dlq.original.offset", str(msg.offset())),
        ("dlq.consumer.group", group),
        ("dlq.failed.at", datetime.now(UTC).isoformat()),
    ]
    producer.send_sync(dlq_topic, msg.key(), msg.value(), headers=headers)


def stop_on_sigterm():
    """`kill` / `docker stop` send SIGTERM: treat it like Ctrl+C so consumers close cleanly (Lesson 02, Q6)."""

    def handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)
