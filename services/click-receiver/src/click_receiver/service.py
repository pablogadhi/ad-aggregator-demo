"""The click path shared by GET /click/{ad_id} and POST /clicks (spec §5.1).

    ad lookup (cache) -> dedup SET NX (1 Redis round trip, 50 ms socket timeout, fail open)
        -> deadline check (shed) -> produce with acks=all + idempotence and WAIT for the ack
        -> answer; on any 503: DEL the dedup key (never "accepted" without an ack)

Delivery semantics: a click we answer `accepted` is in Kafka on min.insync.replicas brokers.
A 503 is one of three outcomes (metric clicks_total{status}):
- shed:      CLICK_DEADLINE_MS had passed before the produce step; nothing produced (definite)
- rejected:  the producer refused it or the broker rejected it (definite: not recorded)
- ambiguous: it timed out *in flight* (librdkafka _MSG_TIMED_OUT, or our capped wait expired after
             handing it to the producer): the broker may have written it, so the client's retry
             may be a second copy — the only over-count source besides dedup failing open.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

import orjson

from click_receiver import metrics
from click_receiver.ads import Ad, AdCache, AdLookupError
from click_receiver.hot import HotTracker
from click_receiver.resilience import RateLimitedLog
from click_receiver.settings import Settings
from click_receiver.store import ClickStore, dedup_key

log = logging.getLogger("click_receiver")
# A broken dependency fails many clicks per second; one line per 10 s per kind (with the number
# suppressed) is enough, the metrics count every click. Log volume competes with the clicks for CPU.
rlog = RateLimitedLog(log, interval=10.0)


class Producer(Protocol):
    async def produce(
        self, topic: str, value: bytes, key: bytes | None = None, *, ack_timeout: float
    ) -> object: ...


class AdNotFound(Exception):
    pass


class ClickNotRecorded(Exception):
    """Kafka did not ack (or the ad could not be looked up): answer 503, the client may retry."""


SHED_DETAIL = "receiver overloaded: click deadline passed before it was recorded, retry"


def possibly_persisted(exc: BaseException) -> bool:
    """An in-flight timeout (sdl_common DeliveryError.possibly_persisted, or a bare timeout from a
    producer without that attribute) is ambiguous; anything else is a definite failure."""
    flag = getattr(exc, "possibly_persisted", None)
    return bool(flag) if flag is not None else isinstance(exc, TimeoutError)


@dataclass(frozen=True, slots=True)
class ClickOutcome:
    click_id: str
    status: Literal["accepted", "duplicate"]
    ad: Ad
    hot: bool
    clicked_at: datetime


def rfc3339_ms(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


class ClickService:
    def __init__(
        self, settings: Settings, ads: AdCache, store: ClickStore, producer: Producer, hot: HotTracker
    ):
        self.s = settings
        self.ads = ads
        self.store = store
        self.producer = producer
        self.hot = hot
        self.deadline = settings.click_deadline_ms / 1000
        self.delivery_timeout = settings.kafka_delivery_timeout_ms / 1000
        # The dedup bound is the Redis client's socket timeout (REDIS_TIMEOUT_MS), which starts
        # when the command is on the wire. A wall-clock timeout here would also count event-loop
        # queueing, so a CPU-saturated receiver "timed out" every dedup and turned dedup off; this
        # outer bound is only a safety net for a stuck connection/pool.
        self.redis_safety_timeout = 4 * settings.redis_timeout_ms / 1000

    async def handle(
        self, ad_id: int, user_id: str, click_id: str, started: float | None = None
    ) -> ClickOutcome:
        """`started` = time.monotonic() when the receiver began handling the request: the
        CLICK_DEADLINE_MS budget is measured from there."""
        if started is None:
            started = time.monotonic()
        # 1. ad lookup (a cache hit never waits; a miss is capped by the budget so a slow DB can't
        #    push the answer past the gateway timeout — the shared lookup itself keeps running)
        ad = self.ads.cached(ad_id)
        if ad is None:
            try:
                async with asyncio.timeout(max(started + self.deadline - time.monotonic(), 0)):
                    ad = await self.ads.get(ad_id)
            except TimeoutError as exc:
                metrics.CLICKS.labels("shed", "false").inc()
                raise ClickNotRecorded(SHED_DETAIL) from exc
            except AdLookupError as exc:
                metrics.AD_LOOKUP_ERRORS.inc()
                raise ClickNotRecorded("ad lookup unavailable") from exc
        if ad is None or not ad.active:
            metrics.UNKNOWN_AD.inc()
            raise AdNotFound(f"ad {ad_id} not found")
        hot = self.hot.is_hot(ad_id)
        hot_label = "true" if hot else "false"

        # 2. dedup: the only Redis round trip on this path
        key = dedup_key(ad_id, user_id)
        claimed_by_us = False
        try:
            async with asyncio.timeout(self.redis_safety_timeout):
                claimed_by_us = await self.store.claim(key, click_id, self.s.dedup_ttl_seconds)
            is_new = claimed_by_us
        except Exception:  # noqa: BLE001 — incl. TimeoutError: fail open (no loss > exact dedup)
            metrics.DEDUP_FAILOPEN.inc()
            is_new = True
        clicked_at = datetime.now(UTC)
        if not is_new:
            metrics.CLICKS.labels("duplicate", hot_label).inc()
            return ClickOutcome(click_id, "duplicate", ad, hot, clicked_at)

        # 3. deadline: past it, producing would only add load for a client that is about to
        #    give up (and push the answer past the gateway's 2 s): shed, nothing produced.
        remaining = started + self.deadline - time.monotonic()
        if remaining <= 0:
            metrics.CLICKS.labels("shed", hot_label).inc()
            if claimed_by_us:
                await self._release(key)
            raise ClickNotRecorded(SHED_DETAIL)

        # 4. produce and wait for the ack (acks=all, idempotent producer), capped by the budget
        salt = self.hot.salt_for(ad_id, hot)
        kafka_key = f"{ad_id}#{salt}" if hot and self.s.hot_salting_enabled else str(ad_id)
        event = {
            "click_id": click_id,
            "ad_id": ad.id,
            "advertiser_id": ad.advertiser_id,
            "salt": salt,
            "user_id": user_id,
            "clicked_at": rfc3339_ms(clicked_at),
            "receiver": self.s.receiver_id,
        }
        start = time.perf_counter()
        try:
            await self.producer.produce(
                self.s.clicks_topic,
                orjson.dumps(event),
                kafka_key.encode(),
                # librdkafka enforces delivery.timeout.ms (1.5 s) itself; the remaining budget
                # can cut the wait shorter, which makes a late ack an ambiguous outcome
                ack_timeout=min(self.delivery_timeout, remaining),
            )
        except Exception as exc:  # noqa: BLE001
            status = "ambiguous" if possibly_persisted(exc) else "rejected"
            metrics.CLICKS.labels(status, hot_label).inc()
            rlog.warning(
                f"not-recorded-{status}",
                "click not recorded",
                extra={"extra_fields": {"ad_id": ad_id, "outcome": status, "error": str(exc)}},
            )
            if claimed_by_us:
                await self._release(key)
            if status == "ambiguous":
                raise ClickNotRecorded("click outcome unknown (Kafka ack timed out), retry") from exc
            raise ClickNotRecorded("click could not be durably recorded, retry") from exc
        metrics.PRODUCE_SECONDS.observe(time.perf_counter() - start)
        metrics.CLICKS.labels("accepted", hot_label).inc()
        self.hot.record(ad_id)
        return ClickOutcome(click_id, "accepted", ad, hot, clicked_at)

    async def _release(self, key: str) -> None:
        """Best effort: let the client's retry count (otherwise it'd be a 'duplicate' of nothing)."""
        try:
            async with asyncio.timeout(max(self.redis_safety_timeout, 0.1)):
                await self.store.release(key)
        except Exception as exc:  # noqa: BLE001
            rlog.warning("release", "dedup key release failed: %s", exc)
