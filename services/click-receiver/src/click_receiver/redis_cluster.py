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
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import Any

from redis.asyncio.cluster import RedisCluster

from click_receiver import metrics

log = logging.getLogger("click_receiver.redis")


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
            if name is not None:
                self._failures.pop(name, None)

    def failure(self, key: str) -> None:
        name = self._node(key) or "?"
        n = self._failures[name] = self._failures.get(name, 0) + 1
        if n >= self.threshold:
            self.request("failures", failed_node=name)

    # -- refreshing ---------------------------------------------------------------------------
    def request(self, reason: str, failed_node: str | None = None) -> bool:
        """Start a background slot-map refresh unless one ran < min_interval ago or is running."""
        now = self.clock()
        if (self._running is not None and not self._running.done()) or now - self._last < self.min_interval:
            return False
        self._last = now
        self._running = asyncio.create_task(self.refresh(reason, failed_node), name="redis-slot-refresh")
        return True

    async def refresh(self, reason: str, failed_node: str | None = None) -> None:
        try:
            await self.client.nodes_manager.initialize(
                last_failed_node_name=None if failed_node == "?" else failed_node
            )
        except Exception as exc:  # noqa: BLE001 — keep the old map; the next trigger retries
            metrics.REDIS_SLOT_REFRESHES.labels(reason, "error").inc()
            log.warning("redis slot map refresh failed (%s): %s", reason, exc)
            return
        metrics.REDIS_SLOT_REFRESHES.labels(reason, "ok").inc()
        if reason != "periodic":
            log.info("redis slot map refreshed (%s, failing node %s)", reason, failed_node)
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
