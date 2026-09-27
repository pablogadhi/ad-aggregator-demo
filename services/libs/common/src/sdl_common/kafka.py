"""Kafka via the `kafka-conn` connection contract (env: KAFKA_BOOTSTRAP_SERVERS).   [extra: kafka]

    from sdl_common.kafka import KafkaSettings, AsyncProducer

    producer = AsyncProducer({"bootstrap.servers": KafkaSettings().bootstrap_servers, "acks": "all", ...})
    producer.start()
    msg = await producer.produce("topic", value=b"...", key=b"...", ack_timeout=2.0)   # waits for the ack
    await producer.close()

Why confluent-kafka (librdkafka) and not aiokafka: librdkafka's idempotent producer is the reference
implementation (epoch bumps, sequence tracking, `delivery.timeout.ms` enforced per message), and its
batching/IO runs in native threads, so the event loop only pays for a C call per message. The one
thing it lacks is asyncio integration, which this module adds: a dedicated thread serves `poll()`
and each delivery report resolves an asyncio future via `loop.call_soon_threadsafe`.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from pydantic_settings import BaseSettings, SettingsConfigDict

log = logging.getLogger("sdl.kafka")


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"


class DeliveryError(Exception):
    """The message was not acknowledged by the broker (error, local queue full, or timeout).

    `possibly_persisted` tells a *definite* failure (False: never handed to / never sent by the
    producer, or rejected by the broker) from an *ambiguous* one (True: the message timed out
    in flight, or our wait for its report expired, so the broker may still have written it).
    `error` is librdkafka's KafkaError when the failure came from a delivery report."""

    def __init__(
        self, message: str, *, error: KafkaError | None = None, possibly_persisted: bool = False
    ) -> None:
        super().__init__(message)
        self.error = error
        self.possibly_persisted = possibly_persisted


# Delivery-report errors after which the broker may have persisted the message (in-flight timeouts,
# or written to the leader without enough in-sync replicas acking it).
IN_FLIGHT_ERRORS = frozenset(
    (
        KafkaError._MSG_TIMED_OUT,
        KafkaError._TIMED_OUT,
        KafkaError.REQUEST_TIMED_OUT,
        KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
    )
)


def _resolve(fut: asyncio.Future, err: KafkaError | None, msg: Message | None) -> None:
    if fut.done():  # the waiter gave up (timeout / cancelled)
        return
    if err is not None:
        fut.set_exception(
            DeliveryError(str(err), error=err, possibly_persisted=err.code() in IN_FLIGHT_ERRORS)
        )
    else:
        fut.set_result(msg)


class AsyncProducer:
    """confluent-kafka Producer with awaitable delivery reports.

    - `produce()` enqueues the message and awaits its delivery report (the broker ack under
      `acks=all`), bounded by `ack_timeout` as a safety net on top of librdkafka's `delivery.timeout.ms`.
    - A fatal error (possible with `enable.idempotence=true`, e.g. an unrecoverable sequence gap)
      makes a librdkafka producer unusable forever: the next `produce()` replaces it with a fresh one.
      In-flight messages of the old producer still get (failed) delivery reports.
    """

    def __init__(self, config: dict[str, Any], *, poll_interval: float = 0.05):
        self._config = {**config, "error_cb": self._on_error}
        self._poll_interval = poll_interval
        self._producer: Producer | None = None
        self._fatal = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    # -- lifecycle -------------------------------------------------------------------------
    def start(self) -> AsyncProducer:
        self._producer = Producer(self._config)
        self._thread = threading.Thread(target=self._poll_loop, name="kafka-poll", daemon=True)
        self._thread.start()
        return self

    async def close(self, flush_timeout: float = 10.0) -> int:
        """Flush outstanding messages (graceful shutdown) and stop the poll thread.
        Returns the number of messages still undelivered."""
        remaining = 0
        if self._producer is not None:
            remaining = await asyncio.to_thread(self._producer.flush, flush_timeout)
        self._stop.set()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, 2.0)
        if remaining:
            log.warning("producer closed with %d undelivered messages", remaining)
        return remaining

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            producer = self._producer
            if producer is not None:
                producer.poll(self._poll_interval)  # serves delivery + error callbacks

    def _on_error(self, err: KafkaError) -> None:
        # Most errors (broker down, all brokers down) are transient: librdkafka retries by itself.
        if err.fatal():
            log.error("fatal producer error, will recreate the producer: %s", err)
            self._fatal.set()
        else:
            log.warning("kafka error: %s", err)

    def _replace_if_fatal(self) -> None:
        if not self._fatal.is_set():
            return
        with self._lock:
            if not self._fatal.is_set():
                return
            old, self._producer = self._producer, Producer(self._config)
            self._fatal.clear()
        if old is not None:
            # purge + serve the remaining callbacks of the dead producer off the event loop
            threading.Thread(target=old.flush, args=(5.0,), daemon=True).start()

    # -- API -------------------------------------------------------------------------------
    async def produce(
        self, topic: str, value: bytes, key: bytes | None = None, *, ack_timeout: float = 5.0
    ) -> Message:
        if self._producer is None:
            raise RuntimeError("AsyncProducer.start() was not called")
        self._replace_if_fatal()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()

        def on_delivery(err: KafkaError | None, msg: Message) -> None:  # runs in the poll thread
            loop.call_soon_threadsafe(_resolve, fut, err, msg)

        try:
            self._producer.produce(topic, value=value, key=key, on_delivery=on_delivery)
        except BufferError as exc:  # local queue full: back-pressure, fail fast
            raise DeliveryError("local producer queue full") from exc
        except KafkaException as exc:
            raise DeliveryError(str(exc)) from exc
        try:
            return await asyncio.wait_for(fut, ack_timeout)
        except TimeoutError as exc:
            # handed to librdkafka and still owned by it: it may yet be delivered -> ambiguous
            raise DeliveryError(f"no delivery report within {ack_timeout}s", possibly_persisted=True) from exc

    async def check(self, topic: str, metadata_timeout: float = 1.5) -> None:
        """Readiness: cluster metadata for `topic` is reachable and the topic has partitions."""
        if self._producer is None:
            raise RuntimeError("producer not started")
        md = await asyncio.to_thread(self._producer.list_topics, topic, metadata_timeout)
        t = md.topics.get(topic)
        if t is None or t.error is not None or not t.partitions:
            raise RuntimeError(f"topic {topic!r} unavailable: {getattr(t, 'error', 'missing')}")
