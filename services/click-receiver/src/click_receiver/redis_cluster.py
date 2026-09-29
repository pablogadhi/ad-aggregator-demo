"""Keeping the per-click Redis Cluster client's slot map fresh (spec §5.1 step 3).

The problem: a Redis primary that dies *silently* (packets dropped, so every command times out
instead of being refused) is failed over by the cluster within seconds, but a client only learns
the new slot owner when it re-reads the slot map. The per-click client runs with no retries and a
50 ms socket timeout, and redis-py's own reaction to a timeout is "re-initialise before the next
command" = CLUSTER SLOTS + COMMAND (a ~200 KB reply) behind a client-wide lock. At hundreds of
timeouts per second that is a CPU storm that stalls every click, not just the ones for the dead
node. So for this client:

- `FastRedisCluster` suppresses redis-py's per-error re-initialisation (after the first one);
- `SlotMapRefresher` counts consecutive failures *per node* (successes on other nodes don't hide
  a dead one) and, after `threshold` of them, re-reads only the slot map
  (`nodes_manager.initialize()`, one CLUSTER SLOTS round trip, the failing node asked last),
  throttled to one refresh per `min_interval` and never two at once;
- the refresher also refreshes every `interval` in the background, which catches failovers and
  resharding that no click happened to trip over.

Standalone Redis needs none of this (there is no slot map): `SlotMapRefresher.for_client()`
returns None for it.

CLUSTERDOWN (measured: the whole cluster in `cluster_state:fail` for 25 min, nodes up but
answering every command with `-CLUSTERDOWN`): redis-py reacts to *each* ClusterDownError /
SlotNotCoveredError (and to every 5th MOVED) with `aclose()` — disconnect every connection to every
node, including the ones other clicks are using — then `sleep(0.25)`, and the next command pays a
full re-initialisation (CLUSTER SLOTS + the ~200 KB COMMAND reply parsed in pure Python, tens of ms
of CPU with the loop blocked) behind a client-wide lock that every click queues on. Hundreds of
clicks/s turned that into a reconnect + re-init storm: every click spent its whole 200 ms safety
timeout in Redis and the loop stalled until the liveness probes timed out. So `FastRedisCluster`
also replaces redis-py's per-command error handling (`_execute_command`): one attempt, a MOVED/ASK
redirect followed once (MOVED patches the slot table in place), and every other error raised as is,
with no client-wide teardown — the caller fails open / backs off, and the refresher (throttled, with
backoff) re-reads the slot map.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import Any

from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.cluster import get_node_name
from redis.exceptions import AskError, MovedError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from click_receiver import metrics
from click_receiver.resilience import RateLimitedLog

log = logging.getLogger("click_receiver.redis")
rlog = RateLimitedLog(log, interval=10.0)


class SlotMapRefresher:
    def __init__(
        self,
        client: Any,  # RedisCluster (duck-typed: get_node_from_key(), nodes_manager.initialize())
        *,
        threshold: int = 3,
        min_interval: float = 1.0,
        interval: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.client = client
        self.threshold = threshold
        self.min_interval = min_interval
        self.interval = interval
        self.clock = clock
        self._failures: dict[str, int] = {}  # node name -> consecutive failures
        self._last = float("-inf")
        # failure-triggered refreshes since the last success: each one that didn't help doubles
        # the gap before the next (min_interval, 2x, 4x, ... capped at the periodic interval), so
        # a cluster that stays down gets CLUSTER SLOTS every ~10 s, not every second
        self._unhelpful = 0
        self._running: asyncio.Task | None = None
        self._periodic: asyncio.Task | None = None

    @classmethod
    def for_client(cls, client: Any, **kwargs: Any) -> SlotMapRefresher | None:
        if not isinstance(client, RedisCluster):
            return None
        refresher = cls(client, **kwargs)
        if isinstance(client, FastRedisCluster):
            client.refresher = refresher
        return refresher

    def _node(self, key: str) -> str | None:
        try:
            node = self.client.get_node_from_key(key)
        except Exception:  # noqa: BLE001 — slot map not loaded yet / slot not covered
            return None
        return node.name if node is not None else None

    # -- called by the store around every per-click command ---------------------------------
    def success(self, key: str) -> None:
        if self._failures:  # the common case (no failures) costs one dict truthiness check
            name = self._node(key)
            if name is not None and self._failures.pop(name, None) is not None:
                self._unhelpful = 0

    def failure(self, key: str) -> None:
        name = self._node(key) or "?"
        n = self._failures[name] = self._failures.get(name, 0) + 1
        if n >= self.threshold:
            self.request("failures", failed_node=name)

    # -- refreshing ---------------------------------------------------------------------------
    @property
    def gap(self) -> float:
        """Minimum time between two refreshes (grows while failure-triggered ones don't help)."""
        if self._unhelpful <= 1:
            return self.min_interval
        return min(
            self.min_interval * 2 ** min(self._unhelpful - 1, 30), max(self.interval, self.min_interval)
        )

    def request(self, reason: str, failed_node: str | None = None) -> bool:
        """Start a background slot-map refresh unless one ran < `gap` ago or is running."""
        now = self.clock()
        if (self._running is not None and not self._running.done()) or now - self._last < self.gap:
            return False
        self._last = now
        if reason == "failures":
            self._unhelpful += 1
        self._running = asyncio.create_task(self.refresh(reason, failed_node), name="redis-slot-refresh")
        return True

    async def refresh(self, reason: str, failed_node: str | None = None) -> None:
        try:
            await self.client.nodes_manager.initialize(
                last_failed_node_name=None if failed_node == "?" else failed_node
            )
        except Exception as exc:  # noqa: BLE001 — keep the old map; the next trigger retries
            metrics.REDIS_SLOT_REFRESHES.labels(reason, "error").inc()
            rlog.warning("refresh-error", "redis slot map refresh failed (%s): %s", reason, exc)
            return
        metrics.REDIS_SLOT_REFRESHES.labels(reason, "ok").inc()
        if reason != "periodic":
            rlog.info("refresh-ok", "redis slot map refreshed (%s, failing node %s)", reason, failed_node)
        # Keep the counters of nodes that still own slots: if they keep failing (cluster down,
        # failover not done yet) every further failure asks again, throttled by `gap`. Forget
        # nodes the new map no longer routes to (their counters could never be reset).
        cache = getattr(self.client.nodes_manager, "nodes_cache", None)
        if isinstance(cache, dict):
            self._failures = {n: c for n, c in self._failures.items() if n in cache}
        else:
            self._failures.clear()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            self.request("periodic")

    def start(self) -> None:
        self._periodic = asyncio.create_task(self._loop(), name="redis-slot-refresh-periodic")

    async def stop(self) -> None:
        for task in (self._periodic, self._running):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task


class FastRedisCluster(RedisCluster):
    """RedisCluster whose re-initialisation after a connection error/timeout is replaced by the
    throttled `SlotMapRefresher` (see the module docstring). The first initialisation (slot map +
    command table) runs normally."""

    refresher: SlotMapRefresher | None = None
    _initialized_once: bool = False

    async def initialize(self, *args: Any, **kwargs: Any) -> RedisCluster:
        if (
            self._initialize
            and self._initialized_once
            and self.refresher is not None
            # after a full aclose() (ClusterDownError path, shutdown) the client needs a real init
            and self.nodes_manager.default_node is not None
        ):
            # redis-py flagged a re-init after an error: skip it; the refresher decides (throttled)
            self._initialize = False
            return self
        await super().initialize(*args, **kwargs)
        self._initialized_once = True
        return self

    async def _execute_command(self, target_node: ClusterNode, *args: Any, **kwargs: Any) -> Any:
        """One attempt on `target_node`; a MOVED/ASK redirect is followed once. Unlike redis-py's
        version, no error tears the client down (`aclose()`), sleeps or flags a full re-init: see
        the module docstring. ClusterDownError, SlotNotCoveredError, TryAgainError, a second
        redirect, ... are raised to the caller as is."""
        redirected = False
        asking = False
        while True:
            try:
                if asking:
                    await target_node.execute_command("ASKING")
                return await target_node.execute_command(*args, **kwargs)
            except (RedisConnectionError, RedisTimeoutError) as e:
                # what redis-py does per node (cheap, lazy): drop this node's idle connections,
                # have the busy ones reconnect when released, and ask it last in the next refresh
                target_node.update_active_connections_for_reconnect()
                await target_node.disconnect_free_connections()
                self.nodes_manager.move_node_to_end_of_cached_nodes(target_node.name)
                e.last_failed_node_name = target_node.name
                raise
            except MovedError as e:
                if redirected:
                    raise
                redirected, asking = True, False
                await self.nodes_manager.move_slot(e)  # patch the slot table in place
                slot = await self._determine_slot(*args)
                target_node = self.nodes_manager.get_node_from_slot(slot)
            except AskError as e:
                if redirected:
                    raise
                redirected, asking = True, True
                node = self.get_node(node_name=get_node_name(host=e.host, port=e.port))
                if node is None:
                    raise
                target_node = node


def build_clients(url: str, mode: str, s: Any) -> tuple[Any, Any, list[SlotMapRefresher]]:
    """The two Redis clients of the receiver (spec §5.1 step 3, §5.1.1) -> (fast, bg, refreshers).

    - fast (per-click dedup): REDIS_TIMEOUT_MS socket/connect timeout, no retries (the caller fails
      open); REDIS_TIMEOUT_MS bounds the socket I/O, not the wall-clock incl. loop queueing.
    - bg (batched hot-ad work): 1 s timeouts, and no client-level retries either: its callers
      (hot.py) keep the batch / the last known set and retry on their own, with backoff. redis-py's
      cluster retry would add aclose() + sleep + a full re-init per attempt (see module docstring).
    In cluster mode both are `FastRedisCluster`s with a `SlotMapRefresher` (start/stop them)."""
    from sdl_common.redis import RedisSettings, connect

    budget = s.redis_timeout_ms / 1000
    fast_kwargs = dict(socket_timeout=budget, socket_connect_timeout=budget, retry=Retry(NoBackoff(), 0))
    bg_kwargs = dict(socket_timeout=1.0, socket_connect_timeout=1.0, retry=Retry(NoBackoff(), 0))
    if mode != "cluster":
        rs = RedisSettings(url=url, mode=mode)
        return connect(rs, **fast_kwargs), connect(rs, **bg_kwargs), []
    fast = FastRedisCluster.from_url(url, decode_responses=True, **fast_kwargs)
    bg = FastRedisCluster.from_url(url, decode_responses=True, **bg_kwargs)
    refreshers = [
        SlotMapRefresher.for_client(
            client,
            threshold=s.redis_slot_refresh_after_failures,
            min_interval=s.redis_slot_refresh_min_interval_ms / 1000,
            interval=s.redis_slot_refresh_interval_ms / 1000,
        )
        for client in (fast, bg)
    ]
    return fast, bg, refreshers
