"""Regression: a Redis Cluster in `cluster_state:fail` (every command answers CLUSTERDOWN) or one
that swallows every command (timeouts) must not starve the event loop (spec §5.1 step 3, §5.1.1).

Measured on the cluster: with Redis answering CLUSTERDOWN, every click-receiver pod failed its
liveness probe (/healthz, which checks nothing) — the event loop was blocked. These tests run the
real redis-py cluster clients (`FastRedisCluster` for clicks, the background client for the hot-ad
loops) against a fake 3-node Redis Cluster speaking RESP3 on real sockets, drive ~200 concurrent
clicks through the ASGI app, and measure event-loop lag and /healthz latency.

The fake server runs in its own thread + event loop so its work doesn't count as our loop's lag.
"""

from __future__ import annotations

import asyncio
import gc
import socket
import threading
import time

import httpx2
import pytest

from click_receiver import metrics
from click_receiver.ads import Ad, AdCache
from click_receiver.hot import HotTracker
from click_receiver.main import app, get_service
from click_receiver.redis_cluster import build_clients
from click_receiver.resilience import NodeBreaker
from click_receiver.service import ClickService
from click_receiver.settings import Settings
from click_receiver.store import RedisClickStore, dedup_key


# -- a tiny RESP3 Redis Cluster ----------------------------------------------------------------
class Simple(str):
    pass


def enc(v) -> bytes:
    if isinstance(v, Simple):
        return b"+" + v.encode() + b"\r\n"
    if isinstance(v, bool):
        return b"#t\r\n" if v else b"#f\r\n"
    if isinstance(v, int):
        return b":%d\r\n" % v
    if isinstance(v, str | bytes):
        b = v.encode() if isinstance(v, str) else v
        return b"$%d\r\n%s\r\n" % (len(b), b)
    if v is None:
        return b"_\r\n"
    if isinstance(v, dict):
        return b"%%%d\r\n" % len(v) + b"".join(enc(k) + enc(x) for k, x in v.items())
    if isinstance(v, set | frozenset):
        return b"~%d\r\n" % len(v) + b"".join(enc(x) for x in sorted(v))
    if isinstance(v, list | tuple):
        return b"*%d\r\n" % len(v) + b"".join(enc(x) for x in v)
    raise TypeError(v)


KEYLESS = {"ping", "cluster", "command", "client", "hello", "info", "readonly"}
REAL = [
    "set",
    "get",
    "del",
    "mget",
    "incrby",
    "expire",
    "eval",
    "evalsha",
    "zadd",
    "zrangebyscore",
    "zremrangebyscore",
    "smembers",
    "sadd",
    "exists",
    "ping",
    "cluster",
    "command",
    "client",
    "hello",
    "info",
    "readonly",
]


def _spec(i: int) -> dict:
    return {
        "notes": f"synthetic key spec {i}",
        "flags": {"RW", "ACCESS", "UPDATE"},
        "begin_search": {"type": "index", "spec": {"index": 1}},
        "find_keys": {"type": "range", "spec": {"lastkey": 0, "keystep": 1, "limit": 0}},
    }


def _entry(name: str, i: int, subs: int) -> list:
    keyless = name in KEYLESS
    first, last = (0, 0) if keyless else (1, -1 if name in ("mget", "del", "exists") else 1)
    return [
        name,
        -2,
        {"write", "denyoom", "fast"},
        first,
        last,
        0 if keyless else 1,
        {"@write", "@string", "@fast", "@keyspace"},
        ["request_policy:all_shards", "response_policy:agg_sum"] if i % 3 == 0 else [],
        [] if keyless else [_spec(i), _spec(i + 1)],
        [_entry(f"{name}|sub{j}", i + j, 0) for j in range(subs)],
    ]


def command_reply() -> bytes:
    """A COMMAND reply shaped like Redis 7's (~240 commands, key specs, sub-commands), ~200 KB."""
    names = REAL + [f"cmd{i}" for i in range(240 - len(REAL))]
    return enc([_entry(n, i, 8 if i % 12 == 0 else 0) for i, n in enumerate(names)])


def parse_commands(buf: bytearray) -> list[list[bytes]]:
    out = []
    while buf:
        if buf[:1] != b"*":
            raise ValueError(bytes(buf[:20]))
        end = buf.find(b"\r\n")
        if end < 0:
            break
        n, pos, args = int(buf[1:end]), end + 2, []
        for _ in range(n):
            e = buf.find(b"\r\n", pos)
            if e < 0:
                return out
            ln = int(buf[pos + 1 : e])
            if len(buf) < e + 2 + ln + 2:
                return out
            args.append(bytes(buf[e + 2 : e + 2 + ln]))
            pos = e + 2 + ln + 2
        else:
            del buf[:pos]
            out.append(args)
            continue
        break
    return out


class FakeCluster:
    """3 masters on 127.0.0.1, each owning a third of the slots. `mode` for keyed commands:
    "ok" (a tiny in-memory store), "clusterdown" (-CLUSTERDOWN), "hang" (never answers)."""

    def __init__(self, n: int = 3):
        self.mode = "ok"
        self.data: dict[bytes, bytes] = {}
        self.counts: dict[str, int] = {}
        self.socks = []
        for _ in range(n):
            s = socket.socket()
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("127.0.0.1", 0))
            s.listen(1024)
            s.setblocking(False)
            self.socks.append(s)
        self.ports = [s.getsockname()[1] for s in self.socks]
        per = 16384 // n
        self.slots = [
            [i * per, 16383 if i == n - 1 else (i + 1) * per - 1, ["127.0.0.1", p, f"node{i}".ljust(40, "0")]]
            for i, p in enumerate(self.ports)
        ]
        self.command = command_reply()
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.servers = [
            self.loop.run_until_complete(asyncio.start_server(self._client, sock=s)) for s in self.socks
        ]
        self.loop.run_forever()
        self.loop.close()

    async def _shutdown(self):
        for server in self.servers:
            server.close()
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.loop.stop()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        asyncio.run_coroutine_threadsafe(self._shutdown(), self.loop)
        self.thread.join(5)

    def count(self, name: str) -> int:
        return self.counts.get(name, 0)

    def reply(self, args: list[bytes]) -> bytes | None:
        cmd = args[0].decode().lower()
        self.counts[cmd] = self.counts.get(cmd, 0) + 1
        if cmd == "hello":
            return enc(
                {
                    "server": "redis",
                    "version": "7.2.4",
                    "proto": 3,
                    "id": 1,
                    "mode": "cluster",
                    "role": "master",
                    "modules": [],
                }
            )
        if cmd == "client" or cmd == "readonly":
            return enc(Simple("OK"))
        if cmd == "ping":
            return enc(Simple("PONG"))
        if cmd == "command":
            return self.command
        if cmd == "cluster":
            return enc(self.slots)
        if self.mode == "clusterdown":
            return b"-CLUSTERDOWN The cluster is down\r\n"
        if self.mode == "hang":
            return None
        if cmd == "set":
            if b"NX" in (a.upper() for a in args) and args[1] in self.data:
                return enc(None)
            self.data[args[1]] = args[2]
            return enc(Simple("OK"))
        if cmd == "del":
            return enc(sum(self.data.pop(k, None) is not None for k in args[1:]))
        if cmd in ("smembers",):
            return enc(set())
        if cmd == "zrangebyscore":
            return enc([])
        if cmd == "mget":
            return enc([None] * (len(args) - 1))
        return enc(0)

    async def _client(self, reader, writer):
        buf = bytearray()
        try:
            while data := await reader.read(65536):
                buf += data
                for args in parse_commands(buf):
                    out = self.reply(args)
                    if out is not None:
                        writer.write(out)
                await writer.drain()
        except (ConnectionError, ValueError, asyncio.CancelledError):
            pass
        finally:
            writer.close()


# -- the receiver wired like main.lifespan, with fake Kafka/Postgres ----------------------------
class OkProducer:
    async def produce(self, topic, value, key=None, *, ack_timeout):
        await asyncio.sleep(0.002)


class Ads:
    async def get(self, ad_id):
        return Ad(ad_id, 1, "https://advertiser.example/", True)


def build(cluster: FakeCluster, **overrides):
    """Wired exactly like main.lifespan (build_clients + NodeBreaker)."""
    s = Settings(pod_name="pod-1", node_name="node-a", **overrides)
    fast, bg, refreshers = build_clients(f"redis://127.0.0.1:{cluster.ports[0]}", "cluster", s)
    breaker = NodeBreaker(
        threshold=s.redis_breaker_after_failures,
        base=s.redis_breaker_open_ms / 1000,
        cap=s.redis_breaker_max_open_ms / 1000,
    )
    store = RedisClickStore(fast, bg, refreshers[0], breaker)
    ads = AdCache(Ads(), 60, 10_000)
    hot = HotTracker(store, s)
    return store, refreshers, hot, ClickService(s, ads, store, OkProducer(), hot)


async def lag_watchdog(samples: list[float], stop: asyncio.Event, tick: float = 0.01):
    while not stop.is_set():
        t0 = time.perf_counter()
        await asyncio.sleep(tick)
        samples.append(time.perf_counter() - t0 - tick)


async def run_load(
    cluster: FakeCluster, mode: str, seconds: float = 3.0, rate: int = 600, concurrency: int = 200
):
    """Warm up against a healthy cluster, switch it to `mode`, then send `rate` clicks/s (open loop,
    at most `concurrency` in flight) for `seconds` while probing /healthz and the loop lag."""
    store, refreshers, hot, service = build(cluster)
    await store.connect()
    await hot.refresh()
    hot.start()
    for refresher in refreshers:
        refresher.start()
    app.dependency_overrides[get_service] = lambda: service
    r = Result()
    claim = store.claim

    async def timed_claim(*args, **kwargs):  # time spent on Redis per click (incl. fail-open)
        t0 = time.perf_counter()
        try:
            return await claim(*args, **kwargs)
        finally:
            r.claims.append(time.perf_counter() - t0)

    store.claim = timed_claim
    stop = asyncio.Event()
    transport = httpx2.ASGITransport(app=app)
    try:
        async with httpx2.AsyncClient(transport=transport, base_url="http://t", timeout=30) as http:
            await http.post("/clicks", json={"ad_id": 1, "user_id": "warm"})
            cluster.mode = mode
            end = time.monotonic() + seconds
            inflight = asyncio.Semaphore(concurrency)

            async def click(i: int):
                try:
                    t0 = time.perf_counter()
                    resp = await http.post("/clicks", json={"ad_id": 1 + i % 50, "user_id": f"u{i}"})
                    r.clicks.append(time.perf_counter() - t0)
                    r.statuses[resp.status_code] = r.statuses.get(resp.status_code, 0) + 1
                finally:
                    inflight.release()

            async def generator():
                tasks, i, t = [], 0, time.monotonic()
                while t < end:
                    t += 1 / rate
                    await asyncio.sleep(max(0.0, t - time.monotonic()))
                    await inflight.acquire()
                    tasks.append(asyncio.create_task(click(i)))
                    i += 1
                await asyncio.gather(*tasks)

            async def prober():
                while time.monotonic() < end:
                    t0 = time.perf_counter()
                    resp = await http.get("/healthz")
                    assert resp.status_code == 200
                    r.health.append(time.perf_counter() - t0)
                    await asyncio.sleep(0.02)

            # the whole test session's garbage would make a full GC pass a lag spike of its own
            gc.collect()
            gc.freeze()
            try:
                dog = asyncio.create_task(lag_watchdog(r.lags, stop))
                await asyncio.gather(prober(), generator())
                stop.set()
                await dog
            finally:
                gc.unfreeze()
    finally:
        app.dependency_overrides.clear()
        await hot.stop(flush_timeout=0.5)
        for refresher in refreshers:
            await refresher.stop()
        for c in (store.fast, store.bg):
            try:
                await asyncio.wait_for(c.aclose(), 2)
            except Exception:  # noqa: BLE001, S110
                pass
    return r


class Result:
    def __init__(self):
        self.lags: list[float] = []
        self.health: list[float] = []
        self.clicks: list[float] = []
        self.claims: list[float] = []
        self.statuses: dict[int, int] = {}

    def __str__(self) -> str:
        ms = lambda xs, q: f"{p(xs, q) * 1000:.1f}ms"  # noqa: E731
        return (
            f"loop lag p50={ms(self.lags, 0.5)} p99={ms(self.lags, 0.99)} max={ms(self.lags, 1)} | "
            f"healthz p50={ms(self.health, 0.5)} max={ms(self.health, 1)} (n={len(self.health)}) | "
            f"clicks n={len(self.clicks)} p50={ms(self.clicks, 0.5)} p99={ms(self.clicks, 0.99)} "
            f"{self.statuses} | redis per click p50={ms(self.claims, 0.5)} max={ms(self.claims, 1)}"
        )


def p(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else float("nan")


# Before the fix (same load: 600 clicks/s for 3 s, <= 200 in flight; this machine):
#   ok           loop lag max 11 ms | Redis time per click p50 0.2 ms          | COMMAND x2, HELLO x14
#   clusterdown  loop lag max 80-113 ms | Redis time per click p50 202 ms (the 200 ms safety timeout,
#                max 281) | COMMAND x15 (full re-inits), HELLO x779 (every aclose() reconnects all)
#   hang         loop lag max 53-102 ms | per click p50 105 ms | HELLO x1815 (a connection per click)
# In the pods (CPU-limited, ~10x the rate) that was a loop stalled past the probes' 1 s timeout.
@pytest.mark.parametrize("mode", ["ok", "clusterdown", "hang"])
async def test_redis_failure_does_not_starve_the_event_loop(mode, capsys):
    with FakeCluster() as cluster:
        async with asyncio.timeout(30):
            failopen0 = metrics.DEDUP_FAILOPEN._value.get()
            r = await run_load(cluster, mode)
        with capsys.disabled():
            print(f"\n{mode}: {r} | server saw {cluster.counts}")
        failed_open = metrics.DEDUP_FAILOPEN._value.get() - failopen0
        assert r.statuses == {200: len(r.clicks)}  # Redis down never fails a click (fail open)
        if mode == "ok":
            assert failed_open == 0
        else:
            assert failed_open >= 0.95 * len(r.clicks)
            # the root cause, pinned: no client teardown + full re-init (COMMAND) per error, no
            # connection per click — only the warm-up's 2 COMMANDs, a bounded number of connects
            assert cluster.count("command") == 2
            assert cluster.count("hello") < 150
            # the breaker keeps all but a few probes off the failing nodes
            assert cluster.count("set") < 0.1 * len(r.clicks)
        assert max(r.lags) < 0.05, "event loop blocked"
        assert max(r.health) < 0.1, "/healthz slow"
        # A click spends at most ~REDIS_TIMEOUT_MS on Redis — 2x when it also has to open a
        # connection (connect/handshake + read timeouts; in this test the fake server shares the
        # GIL, so handshakes are slow) — and only the first wave, before the breakers open.
        # Never the 200 ms safety timeout (before the fix: p50 202 ms).
        slow = sum(c > 0.06 for c in r.claims)
        assert slow < 0.05 * len(r.claims), f"{slow} clicks waited on Redis beyond REDIS_TIMEOUT_MS"
        assert max(r.claims) < 0.18


async def test_dedup_resumes_when_the_cluster_recovers():
    """CLUSTERDOWN -> breakers open, hot loops back off; cluster back -> within a few seconds the
    probes close the breakers and duplicates are detected again; the hot loop flushes again."""
    with FakeCluster() as cluster:
        store, refreshers, hot, service = build(cluster, hot_flush_interval_ms=100, redis_breaker_open_ms=100)
        try:
            async with asyncio.timeout(20):
                await store.connect()
                hot.start()
                cluster.mode = "clusterdown"
                for i in range(50):
                    out = await service.handle(1, f"u{i}", f"c{i}")
                    assert out.status == "accepted"
                assert not store.breaker.idle  # opened
                await asyncio.sleep(0.5)
                flushes_down = cluster.count("incrby")
                cluster.mode = "ok"
                deadline = time.monotonic() + 10
                while True:
                    await service.handle(1, "again", "c-a")
                    if (await service.handle(1, "again", "c-b")).status == "duplicate":
                        break
                    assert time.monotonic() < deadline, "dedup never resumed"
                    await asyncio.sleep(0.05)
                assert not store.breaker.is_open(store._node(dedup_key(1, "again")))
                # while down the 100 ms flush loop backed off (0.2, 0.4, 0.8 s ...): a handful of
                # attempts in ~0.6 s instead of ~6; once back it flushes again (after at most the
                # current backoff delay) and returns to its interval
                assert flushes_down <= 4
                t0 = cluster.count("incrby")
                while cluster.count("incrby") == t0:
                    assert time.monotonic() < deadline + 5, "flush loop never recovered"
                    await asyncio.sleep(0.05)
                await asyncio.sleep(0.05)
                assert hot._pending == {}  # the counts kept while down were flushed
                assert metrics.HOT_LOOP_BACKOFF.labels("flush")._value.get() == 0.1  # interval again
        finally:
            await hot.stop(flush_timeout=0.5)
            for c in (store.fast, store.bg):
                await asyncio.wait_for(c.aclose(), 2)
