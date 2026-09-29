"""Backoff, the per-node dedup breaker, rate-limited logs, the hot loops' and slot-map refresher's
backoff, and FastRedisCluster's error handling (no client teardown on CLUSTERDOWN)."""

import asyncio
import logging

import pytest
from prometheus_client import REGISTRY
from redis.exceptions import ClusterDownError, MovedError

from click_receiver.hot import HotTracker
from click_receiver.redis_cluster import FastRedisCluster, SlotMapRefresher
from click_receiver.resilience import Backoff, NodeBreaker, RateLimitedLog
from click_receiver.settings import Settings
from click_receiver.store import RedisClickStore, RedisUnavailable


class Clock:
    def __init__(self, t: float = 100.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


def metric(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


# -- Backoff --------------------------------------------------------------------------------
def test_backoff_doubles_is_capped_and_resets():
    b = Backoff(0.5, 4.0, jitter=0)
    assert [b.next() for _ in range(6)] == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]
    b.reset()
    assert b.next() == 0.5


def test_backoff_jitter_stays_within_bounds():
    lo = Backoff(1.0, 10.0, jitter=0.2, rand=lambda: 0.0)
    hi = Backoff(1.0, 10.0, jitter=0.2, rand=lambda: 1.0)
    assert lo.next() == pytest.approx(0.8) and hi.next() == pytest.approx(1.2)


# -- NodeBreaker ----------------------------------------------------------------------------
def test_breaker_opens_after_consecutive_failures_and_probes_with_backoff():
    clock = Clock()
    b = NodeBreaker(threshold=3, base=0.5, cap=2.0, clock=clock)
    assert b.idle
    for _ in range(2):
        assert not b.failure("A")
    b.success("A")  # a success resets the count
    for _ in range(2):
        b.failure("A")
    assert b.allow("A")
    assert b.failure("A")  # 3rd consecutive: open
    assert not b.allow("A") and b.allow("B")  # other nodes unaffected
    assert b.failure("A")  # a call already in flight fails: no extra backoff
    clock.t += 0.61  # 0.5 s window (+-20 % jitter) over
    assert b.allow("A")  # half-open: this caller probes...
    assert not b.allow("A")  # ...everyone else keeps skipping
    b.failure("A")  # probe failed: next window is ~1 s
    clock.t += 0.61
    assert not b.allow("A")
    clock.t += 0.61
    assert b.allow("A")
    b.success("A")  # probe ok: closed
    assert b.allow("A") and b.allow("A") and b.idle


def test_breaker_window_is_capped():
    clock = Clock()
    b = NodeBreaker(threshold=1, base=0.5, cap=2.0, clock=clock)
    b.failure("A")
    for _ in range(10):  # probes keep failing
        clock.t += 10
        assert b.allow("A")
        b.failure("A")
    clock.t += 2.41  # cap 2 s (+20 % jitter)
    assert b.allow("A")


class FakeFast:
    """A cluster client whose every SET fails (like CLUSTERDOWN); keys 'a*' on node A."""

    def __init__(self):
        self.calls = 0
        self.error: Exception | None = ClusterDownError("CLUSTERDOWN The cluster is down")

    def get_node_from_key(self, key):
        return type("Node", (), {"name": "A" if key[0] == "a" else "B"})()

    async def set(self, *args, **kwargs):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return True

    async def delete(self, key):
        self.calls += 1


async def test_store_skips_redis_while_the_node_breaker_is_open():
    async with asyncio.timeout(5):
        clock = Clock()
        fast = FakeFast()
        store = RedisClickStore(fast, fast, None, NodeBreaker(threshold=3, base=0.5, cap=5, clock=clock))
        skips = metric("click_redis_breaker_skips_total")
        for i in range(10):
            with pytest.raises((ClusterDownError, RedisUnavailable)):
                await store.claim(f"a{i}", "c", 60)
        assert fast.calls == 3  # then skipped without a call
        assert metric("click_redis_breaker_skips_total") == skips + 7
        await store.release("a1")  # best effort: skipped too
        assert fast.calls == 3
        with pytest.raises(ClusterDownError):
            await store.claim("b1", "c", 60)  # node B is still asked
        assert fast.calls == 4
        fast.error = None
        clock.t += 1
        assert await store.claim("a1", "c", 60)  # the probe succeeds: closed
        assert not store.breaker.is_open("A")
        assert await store.claim("b2", "c", 60)  # B's failure count is reset by a success
        assert store.breaker.idle


async def test_failed_open_click_via_breaker_is_counted_as_fail_open(h):
    async with asyncio.timeout(5):
        fast = FakeFast()
        h.service.store = RedisClickStore(fast, fast, None, NodeBreaker(threshold=1))
        before = metric("click_dedup_failopen_total")
        for i in range(5):
            assert (await h.service.handle(42, f"u{i}", f"c{i}")).status == "accepted"
        assert metric("click_dedup_failopen_total") == before + 5
        assert fast.calls == 1


# -- RateLimitedLog -------------------------------------------------------------------------
def test_rate_limited_log(caplog):
    clock = Clock()
    rl = RateLimitedLog(logging.getLogger("t.rl"), interval=10, clock=clock)
    with caplog.at_level(logging.WARNING, logger="t.rl"):
        assert rl.warning("k", "redis down: %s", "x")
        for _ in range(99):
            assert not rl.warning("k", "redis down: %s", "x")
        assert rl.warning("other", "different kind")  # keys are independent
        clock.t += 10
        assert rl.warning("k", "redis down: %s", "y")
    msgs = [r.getMessage() for r in caplog.records]
    assert msgs == [
        "redis down: x",
        "different kind",
        "redis down: y (99 similar suppressed in the last 10s)",
    ]


# -- hot loops back off -----------------------------------------------------------------------
async def test_hot_loop_backs_off_exponentially_and_recovers(monkeypatch):
    async with asyncio.timeout(5):
        delays: list[float] = []
        results = iter([False, False, False, False, False, False, True, True])
        done = asyncio.Event()
        real_sleep = asyncio.sleep

        async def fake_sleep(d):
            delays.append(d)
            await real_sleep(0)

        async def step():
            try:
                return next(results)
            except StopIteration:
                done.set()
                return True

        monkeypatch.setattr("click_receiver.hot.asyncio.sleep", fake_sleep)
        monkeypatch.setattr("click_receiver.resilience.random.random", lambda: 0.5)  # no jitter
        hot = HotTracker(None, Settings(hot_backoff_max_ms=10_000))
        task = asyncio.create_task(hot._loop("flush", step, 1000))
        await done.wait()
        task.cancel()
        # interval, then 2x, 4x, 8x, capped at 10 s while failing; back to the interval on success
        assert delays[:9] == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0, 10.0, 1.0, 1.0]


async def test_hot_steps_report_failure(h):
    async with asyncio.timeout(5):
        h.hot.record(42)
        assert await h.hot.flush() is True
        h.store.fail.update({"add_counts", "load_hot"})
        h.hot.record(42)
        assert await h.hot.flush() is False
        assert await h.hot.refresh() is False
        assert await h.hot.flush() is False  # the kept batch is retried (and fails again)


# -- slot-map refresher backs off while refreshes don't help ------------------------------------
class FakeNodes:
    def __init__(self):
        self.refreshes = 0
        self.nodes_cache = {"A": object(), "B": object()}

    async def initialize(self, last_failed_node_name=None):
        self.refreshes += 1


class FakeCluster:
    def __init__(self):
        self.nodes_manager = FakeNodes()

    def get_node_from_key(self, key):
        return type("Node", (), {"name": "A" if key[0] == "a" else "B"})()


async def test_refresher_backs_off_while_the_cluster_stays_down():
    async with asyncio.timeout(5):
        clock = Clock()
        cluster = FakeCluster()
        r = SlotMapRefresher(cluster, threshold=3, min_interval=1.0, interval=10.0, clock=clock)
        times = []
        for _ in range(300):  # CLUSTERDOWN: a failure every 0.1 s for 30 s
            before = cluster.nodes_manager.refreshes
            r.failure("a")
            await asyncio.sleep(0)
            if cluster.nodes_manager.refreshes > before:
                times.append(clock.t)
            clock.t += 0.1
        # 1 s, then 2, 4, 8, and every 10 s (the periodic interval caps it)
        gaps = [b - a for a, b in zip(times, times[1:], strict=False)]  # (failures come every 0.1 s)
        assert gaps[:4] == pytest.approx([1.0, 2.0, 4.0, 8.0], abs=0.15)
        assert gaps[4:] == pytest.approx([10.0] * len(gaps[4:]), abs=0.15) and len(gaps) >= 5
        r.success("a1")  # the node answers again: back to the base gap
        assert r.gap == 1.0


# -- FastRedisCluster: no client-wide teardown on CLUSTERDOWN ----------------------------------
class Node:
    def __init__(self, name, error=None):
        self.name = name
        self.error = error
        self.calls = []

    async def execute_command(self, *args, **kwargs):
        self.calls.append(args)
        if self.error is not None:
            raise self.error
        return "OK"


async def test_fast_cluster_raises_clusterdown_without_teardown_or_sleep(monkeypatch):
    async with asyncio.timeout(5):
        client = FastRedisCluster(host="127.0.0.1", port=1)
        closed = []

        async def aclose():
            closed.append(1)

        monkeypatch.setattr(client, "aclose", aclose)
        node = Node("n1", ClusterDownError("CLUSTERDOWN The cluster is down"))
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        with pytest.raises(ClusterDownError):
            await client._execute_command(node, "SET", "k", "v")
        assert loop.time() - t0 < 0.05  # redis-py sleeps 0.25 s here
        assert closed == [] and client._initialize is True  # untouched (True = never initialised)
        assert len(node.calls) == 1


async def test_fast_cluster_follows_one_moved_redirect(monkeypatch):
    async with asyncio.timeout(5):
        client = FastRedisCluster(host="127.0.0.1", port=1)
        new = Node("n2")
        old = Node("n1", MovedError("3999 127.0.0.1:7001"))
        moved = []

        async def move_slot(e):
            moved.append(e)

        async def determine_slot(*args):
            return 3999

        nodes = type(
            "Nodes",
            (),
            {"move_slot": staticmethod(move_slot), "get_node_from_slot": staticmethod(lambda slot: new)},
        )()
        monkeypatch.setattr(client, "nodes_manager", nodes)
        monkeypatch.setattr(client, "_determine_slot", determine_slot)
        assert await client._execute_command(old, "SET", "k", "v") == "OK"
        assert len(moved) == 1 and new.calls == [("SET", "k", "v")]
        # a second MOVED is not chased (the caller fails open; the refresher catches up)
        new.error = MovedError("3999 127.0.0.1:7002")
        with pytest.raises(MovedError):
            await client._execute_command(old, "SET", "k", "v")
