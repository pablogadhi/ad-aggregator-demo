import asyncio
from contextlib import asynccontextmanager

import pytest
from confluent_kafka import Producer
from sdl_common.kafka import AsyncProducer, DeliveryError

from click_receiver.ads import Ad, AdCache, AdLookupError, PostgresAds
from click_receiver.main import producer_config
from click_receiver.settings import Settings

AD = Ad(1, 2, "https://x.example", True)


# -- AdCache ------------------------------------------------------------------------------------
async def test_cache_ttl_and_coalescing(h):
    cache, source = h.ads, h.source
    source.delay = 0.05
    results = await asyncio.gather(*(cache.get(42) for _ in range(20)))
    assert all(r.id == 42 for r in results)
    assert source.calls == 1  # 20 concurrent misses, one query
    await cache.get(42)
    assert source.calls == 1


async def test_cache_expires_and_does_not_cache_unknown_ids():
    clock = [0.0]
    ads = {}

    class Source:
        calls = 0

        async def get(self, ad_id):
            Source.calls += 1
            return ads.get(ad_id)

    cache = AdCache(Source(), ttl=60, max_entries=2, clock=lambda: clock[0])
    assert await cache.get(1) is None
    ads[1] = AD  # created right after a 404: clickable at once
    assert await cache.get(1) == AD
    clock[0] = 61
    await cache.get(1)
    assert Source.calls == 3
    # bounded
    ads[2] = ads[3] = AD
    await cache.get(2)
    await cache.get(3)
    assert len(cache) == 2


# -- PostgresAds: replica -> primary --------------------------------------------------------
class FakePool:
    def __init__(self, rows, fail=False):
        self.rows, self.fail, self.queries = rows, fail, 0

    @asynccontextmanager
    async def connection(self, timeout=None):  # noqa: ASYNC109 — mirrors psycopg_pool
        if self.fail:
            raise OSError("pool down")
        pool = self

        class Conn:
            async def execute(self, sql, params=()):
                pool.queries += 1
                row = pool.rows.get(params[0]) if params else {"?column?": 1}

                class Cur:
                    async def fetchone(self):
                        return row

                return Cur()

        yield Conn()


class FakeDB:
    def __init__(self, primary, replica):
        self.primary, self.replica = primary, replica


ROW = {"id": 5, "advertiser_id": 9, "redirect_url": "https://r.example", "active": True}


async def test_replica_miss_falls_back_to_primary():
    primary, replica = FakePool({5: ROW}), FakePool({})  # replica lags
    ads = PostgresAds(FakeDB(primary, replica), timeout=1)
    assert await ads.get(5) == Ad(5, 9, "https://r.example", True)
    assert (replica.queries, primary.queries) == (1, 1)
    assert await ads.get(6) is None


async def test_replica_hit_does_not_touch_primary():
    primary, replica = FakePool({5: ROW}), FakePool({5: ROW})
    await PostgresAds(FakeDB(primary, replica), timeout=1).get(5)
    assert primary.queries == 0


async def test_replica_down_uses_primary_and_both_down_raises():
    primary, replica = FakePool({5: ROW}), FakePool({}, fail=True)
    ads = PostgresAds(FakeDB(primary, replica), timeout=1)
    assert (await ads.get(5)).id == 5
    await ads.check()  # ready: the primary answers
    primary.fail = True
    with pytest.raises(AdLookupError):
        await ads.get(5)
    with pytest.raises(RuntimeError):
        await ads.check()


async def test_readiness_survives_primary_failover():
    primary, replica = FakePool({}, fail=True), FakePool({})
    await PostgresAds(FakeDB(primary, replica), timeout=1).check()


# -- Kafka producer -----------------------------------------------------------------------------
def test_producer_config_is_valid_for_librdkafka():
    cfg = producer_config("localhost:9092", "PLAINTEXT", Settings())
    assert cfg["acks"] == "all" and cfg["enable.idempotence"] is True
    assert cfg["delivery.timeout.ms"] == 1500
    Producer(cfg)  # raises KafkaException on an invalid/contradictory config


async def test_async_producer_reports_delivery_failure():
    """The asyncio bridge surfaces librdkafka's delivery timeout as DeliveryError (no broker)."""
    cfg = producer_config("127.0.0.1:1", "PLAINTEXT", Settings(kafka_delivery_timeout_ms=300))
    producer = AsyncProducer(cfg).start()
    try:
        with pytest.raises(DeliveryError):
            await producer.produce("clicks", b"{}", b"1", ack_timeout=3)
        with pytest.raises(Exception):  # noqa: B017
            await producer.check("clicks", metadata_timeout=0.3)
    finally:
        await producer.close(flush_timeout=1)
