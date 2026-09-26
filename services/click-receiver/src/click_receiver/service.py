"""The click path shared by GET /click/{ad_id} and POST /clicks (spec §5.1).

    ad lookup (cache) -> dedup SET NX (1 Redis round trip, 50 ms, fail open)
        -> produce with acks=all + idempotence and WAIT for the ack -> answer
        -> on produce failure: DEL the dedup key, 503 (never "accepted" without an ack)

Delivery semantics: a click we answer `accepted` is in Kafka on min.insync.replicas brokers.
A 503 means "not known to be recorded": if the broker wrote it but the ack was lost, the client's
retry is a second (distinct) click — the only over-count source besides dedup failing open.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

from click_receiver import metrics
from click_receiver.ads import Ad, AdCache, AdLookupError
from click_receiver.hot import HotTracker
from click_receiver.settings import Settings
from click_receiver.store import ClickStore, dedup_key

log = logging.getLogger("click_receiver")


class Producer(Protocol):
    async def produce(
        self, topic: str, value: bytes, key: bytes | None = None, *, ack_timeout: float
    ) -> object: ...


class AdNotFound(Exception):
    pass


class ClickNotRecorded(Exception):
    """Kafka did not ack (or the ad could not be looked up): answer 503, the client may retry."""


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

    async def handle(self, ad_id: int, user_id: str, click_id: str) -> ClickOutcome:
        # 1. ad lookup
        try:
            ad = await self.ads.get(ad_id)
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
            async with asyncio.timeout(self.s.redis_timeout_ms / 1000):
                claimed_by_us = await self.store.claim(key, click_id, self.s.dedup_ttl_seconds)
            is_new = claimed_by_us
        except Exception:  # noqa: BLE001 — incl. TimeoutError: fail open (no loss > exact dedup)
            metrics.DEDUP_FAILOPEN.inc()
            is_new = True
        clicked_at = datetime.now(UTC)
        if not is_new:
            metrics.CLICKS.labels("duplicate", hot_label).inc()
            return ClickOutcome(click_id, "duplicate", ad, hot, clicked_at)

        # 3. produce and wait for the ack (acks=all, idempotent producer)
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
                json.dumps(event, separators=(",", ":")).encode(),
                kafka_key.encode(),
                # librdkafka enforces delivery.timeout.ms itself; this is only a safety net
                ack_timeout=self.s.kafka_delivery_timeout_ms / 1000 + 0.3,
            )
        except Exception as exc:  # noqa: BLE001
            metrics.CLICKS.labels("rejected", hot_label).inc()
            log.warning("click not recorded", extra={"extra_fields": {"ad_id": ad_id, "error": str(exc)}})
            if claimed_by_us:
                await self._release(key)
            raise ClickNotRecorded("click could not be durably recorded, retry") from exc
        metrics.PRODUCE_SECONDS.observe(time.perf_counter() - start)
        metrics.CLICKS.labels("accepted", hot_label).inc()
        self.hot.record(ad_id)
        return ClickOutcome(click_id, "accepted", ad, hot, clicked_at)

    async def _release(self, key: str) -> None:
        """Best effort: let the client's retry count (otherwise it'd be a 'duplicate' of nothing)."""
        try:
            async with asyncio.timeout(max(self.s.redis_timeout_ms, 100) / 1000):
                await self.store.release(key)
        except Exception as exc:  # noqa: BLE001
            log.warning("dedup key release failed: %s", exc)
