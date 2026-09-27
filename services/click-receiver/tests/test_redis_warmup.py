"""Slot-map refresh of the per-click Redis Cluster client, and the warm-up before readiness."""

import asyncio

import pytest
from prometheus_client import REGISTRY
from redis.asyncio.cluster import RedisCluster

from click_receiver.ads import Ad, AdCache, AdLookupError
from click_receiver.redis_cluster import FastRedisCluster, SlotMapRefresher
from click_receiver.store import RedisClickStore
from click_receiver.warmup import Warmup


# -- SlotMapRefresher -------------------------------------------------------------------------
class FakeNodes:
    def __init__(self):
        self.refreshes: list[str | None] = []
        self.fail = False

    async def initialize(self, last_failed_node_name=None):
        self.refreshes.append(last_failed_node_name)
        if self.fail:
            raise ConnectionError("no node answers")


class FakeCluster:
    """Keys 'a*' live on node A, 'b*' on node B."""

    def __init__(self):
        self.nodes_manager = FakeNodes()

    def get_node_from_key(self, key):
        return type("Node", (), {"name": f"10.0.0.{'1' if key[0] == 'a' else '2'}:6379"})()


class Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def make_refresher():
    clock = Clock()
    cluster = FakeCluster()
    return SlotMapRefresher(cluster, threshold=3, min_interval=1.0, interval=10, clock=clock), cluster, clock


async def settle():
    for _ in range(3):
        await asyncio.sleep(0)


async def test_refresh_after_n_consecutive_failures_on_a_node():
    r, cluster, _ = make_refresher()
    r.failure("a1")
    r.failure("a2")
    r.success("b1")  # another node answering does not hide node A's failures
    await settle()
    assert cluster.nodes_manager.refreshes == []
    r.failure("a3")
    await settle()
    # one CLUSTER SLOTS refresh, asking the failing node last
    assert cluster.nodes_manager.refreshes == ["10.0.0.1:6379"]


async def test_success_resets_the_node_counter():
    r, cluster, _ = make_refresher()
    r.failure("a1")
    r.failure("a2")
    r.success("a3")
    r.failure("a4")
    r.failure("a5")
    await settle()
    assert cluster.nodes_manager.refreshes == []


async def test_refresh_is_throttled_to_one_per_interval():
    r, cluster, clock = make_refresher()
    cluster.nodes_manager.fail = True  # the node is still being failed over: failures continue
    for i in range(20):
        r.failure(f"a{i}")
        await settle()
    assert len(cluster.nodes_manager.refreshes) == 1
    clock.t += 0.5
    r.failure("a99")
    await settle()
    assert len(cluster.nodes_manager.refreshes) == 1
    clock.t += 0.6  # > 1 s since the last refresh
    r.failure("a100")
    await settle()
    assert len(cluster.nodes_manager.refreshes) == 2
    before = REGISTRY.get_sample_value(
        "click_redis_slot_refreshes_total", {"reason": "failures", "result": "error"}
    )
    assert before >= 2


async def test_no_concurrent_refreshes():
    r, cluster, clock = make_refresher()
    gate = asyncio.Event()

    async def slow(last_failed_node_name=None):
        cluster.nodes_manager.refreshes.append(last_failed_node_name)
        await gate.wait()

    cluster.nodes_manager.initialize = slow
    assert r.request("failures")
    await settle()
    clock.t += 5
    assert not r.request("periodic")  # still running
    gate.set()
    await settle()
    assert r.request("periodic")
    await r.stop()


async def test_periodic_refresh():
    r, cluster, clock = make_refresher()
    r.interval = 0.01
    r.start()
    await asyncio.sleep(0.05)
    await r.stop()
    assert len(cluster.nodes_manager.refreshes) == 1  # throttled: clock did not advance


def test_standalone_needs_no_refresher():
    import fakeredis

    assert SlotMapRefresher.for_client(fakeredis.FakeAsyncRedis()) is None


async def test_fast_cluster_client_skips_redis_py_reinit_after_errors():
    """redis-py re-initialises (CLUSTER SLOTS + COMMAND) before the next command after every
    timeout; the per-click client leaves that to the throttled refresher."""
    client = FastRedisCluster(host="127.0.0.1", port=1)
    SlotMapRefresher.for_client(client)
    assert client.refresher is not None
    client._initialized_once = True
    client.nodes_manager.default_node = object()  # stands in for a loaded slot map
    client._initialize = True  # what redis-py sets after a ConnectionError/TimeoutError
    assert await asyncio.wait_for(client.initialize(), 0.5) is client  # no network I/O
    assert client._initialize is False
    # a plain RedisCluster would have tried to connect
    plain = RedisCluster(host="127.0.0.1", port=1, socket_connect_timeout=0.05)
    with pytest.raises(Exception):  # noqa: B017
        await plain.initialize()


async def test_store_reports_claim_outcomes_to_the_refresher():
    import fakeredis

    class Recorder:
        def __init__(self):
            self.events = []

        def success(self, key):
            self.events.append(("ok", key))

        def failure(self, key):
            self.events.append(("fail", key))

    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    rec = Recorder()
    store = RedisClickStore(redis, redis, rec)
    assert await store.claim("k1", "c", 60)
    await redis.aclose()

    class Down:
        async def set(self, *args, **kwargs):
            raise TimeoutError

    store.fast = Down()
    with pytest.raises(TimeoutError):
        await store.claim("k2", "c", 60)
    assert rec.events == [("ok", "k1"), ("fail", "k2")]


# -- warm-up ------------------------------------------------------------------------------------
async def test_warmup_gates_readiness_and_skips_failed_steps():
    calls = []
    gate = asyncio.Event()

    async def kafka():
        calls.append("kafka")
        await gate.wait()

    async def redis_down():
        calls.append("redis")
        await asyncio.sleep(10)  # Redis unreachable: skipped after its short timeout

    flaky = {"n": 0}

    async def ads():
        flaky["n"] += 1
        if flaky["n"] == 1:
            raise OSError("replica restarting")
        return "3 ads"

    w = Warmup([("kafka", kafka, 1, 3), ("redis", redis_down, 0.05, 1), ("ad_cache", ads, 1, 3)])
    w.start()
    await settle()
    with pytest.raises(RuntimeError, match="warming up"):
        await w.check()
    gate.set()
    await asyncio.wait_for(w.done.wait(), 3)
    await w.check()  # ready
    assert calls == ["kafka", "redis"]
    assert w.results["kafka"].startswith("ok")
    assert w.results["redis"].startswith("skipped: TimeoutError")
    assert w.results["ad_cache"].startswith("ok (3 ads") and flaky["n"] == 2


async def test_ad_cache_preload_is_bounded_and_served_without_db():
    class NoDB:
        async def get(self, ad_id):
            raise AssertionError("preloaded ads must not hit the DB")

    cache = AdCache(NoDB(), ttl=60, max_entries=3)
    ads = [Ad(i, 1, f"https://r.example/{i}", True) for i in range(1, 6)]
    assert cache.preload(ads) == 3 and len(cache) == 3
    assert cache.cached(1) == ads[0] and await cache.get(2) == ads[1]
    assert cache.cached(4) is None


async def test_postgres_preload_uses_the_replica_then_the_primary():
    from contextlib import asynccontextmanager

    from click_receiver.ads import PostgresAds

    class Pool:
        def __init__(self, rows, fail=False):
            self.rows, self.fail, self.sql = rows, fail, []

        @asynccontextmanager
        async def connection(self, timeout=None):  # noqa: ASYNC109
            if self.fail:
                raise OSError("down")
            pool = self

            class Conn:
                async def execute(self, sql, params=()):
                    pool.sql.append((sql, params))

                    class Cur:
                        async def fetchall(self):
                            return pool.rows

                    return Cur()

            yield Conn()

    row = {"id": 5, "advertiser_id": 9, "redirect_url": "https://r.example", "active": True}
    replica, primary = Pool([row]), Pool([])
    repo = PostgresAds(type("DB", (), {"primary": primary, "replica": replica})(), timeout=1)
    assert await repo.preload(50_000) == [Ad(5, 9, "https://r.example", True)]
    ((sql, params),) = replica.sql
    assert "WHERE active" in sql and "LIMIT" in sql and params == (50_000,) and primary.sql == []
    replica.fail = True
    assert await repo.preload(10) == []  # primary answered
    primary.fail = True
    with pytest.raises(AdLookupError):
        await repo.preload(10)
