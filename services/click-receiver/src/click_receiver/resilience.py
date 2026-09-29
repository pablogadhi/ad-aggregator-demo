"""Small resilience helpers for the Redis side of click-receiver (spec §5.1 step 3, §5.1.1).

Why they exist: with the Redis Cluster in `cluster_state:fail` (every command answered
CLUSTERDOWN) or silently dead (every command timing out), the receiver must fail open *cheaply*.
Measured before these helpers: each click re-built the whole cluster client or opened a fresh
connection, the event loop stalled and the liveness probe (/healthz) timed out. So:

- `Backoff`: capped exponential backoff with jitter, for everything that retries on a timer;
- `NodeBreaker`: a per-node circuit breaker for the per-click dedup — after N consecutive
  failures on a node, its keys skip Redis (fail open at ~0 cost) for an exponentially growing,
  capped window; then one click probes it (half-open) while the others keep skipping;
- `RateLimitedLog`: one log line per key per interval, carrying how many were suppressed (the
  metrics still count every event), so a failure at thousands of clicks/s doesn't become
  thousands of JSON log lines/s competing with the clicks for the loop.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass

from click_receiver import metrics


class Backoff:
    """base, 2*base, 4*base, ... capped at `cap`, each with +-`jitter` (fraction) so many pods
    don't retry in lockstep. `reset()` after a success."""

    def __init__(self, base: float, cap: float, jitter: float = 0.2, rand: Callable[[], float] | None = None):
        self.base = base
        self.cap = max(cap, base)
        self.jitter = jitter
        self.rand = rand or random.random
        self.failures = 0

    @property
    def current(self) -> float:
        """The un-jittered delay the next `next()` returns."""
        return min(self.cap, self.base * 2 ** min(self.failures, 30))

    def next(self) -> float:
        delay = self.current
        self.failures += 1
        return delay * (1 + self.jitter * (2 * self.rand() - 1))

    def reset(self) -> None:
        self.failures = 0


@dataclass(slots=True)
class _Open:
    until: float
    backoff: Backoff
    probing: bool = False


class NodeBreaker:
    """Per Redis node (cluster: the key's slot owner; standalone: one node).

    closed -> `threshold` consecutive failures -> open (skip) for `backoff` -> half-open: the next
    caller probes, everyone else keeps skipping -> probe ok: closed / probe failed: open for the
    next (doubled, capped) window. The common case (nothing failing) costs two dict truthiness
    checks per click."""

    def __init__(
        self,
        threshold: int = 5,
        base: float = 0.5,
        cap: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.threshold = threshold
        self.base = base
        self.cap = cap
        self.clock = clock
        self._failures: dict[str, int] = {}  # closed nodes with consecutive failures
        self._open: dict[str, _Open] = {}

    @property
    def idle(self) -> bool:
        """True when no node has failures or is open: callers may skip resolving the node."""
        return not self._failures and not self._open

    def is_open(self, node: str) -> bool:
        """Open or half-open (without taking the probe slot)."""
        return node in self._open

    def allow(self, node: str) -> bool:
        st = self._open.get(node)
        if st is None:
            return True
        if st.probing or self.clock() < st.until:
            metrics.REDIS_BREAKER_SKIPS.inc()
            return False
        st.probing = True  # half-open: this caller is the probe
        return True

    def success(self, node: str) -> None:
        self._failures.pop(node, None)
        if self._open.pop(node, None) is not None:
            metrics.REDIS_BREAKER_OPEN.set(len(self._open))

    def failure(self, node: str) -> bool:
        """Record a failure; returns True when the node is (now) open."""
        st = self._open.get(node)
        if st is not None:
            if st.probing:  # the probe failed: re-open for the next, longer window
                st.probing = False
                st.until = self.clock() + st.backoff.next()
            # else: a call already in flight when the breaker opened; it says nothing new
            return True
        n = self._failures[node] = self._failures.get(node, 0) + 1
        if n < self.threshold:
            return False
        del self._failures[node]
        backoff = Backoff(self.base, self.cap)
        self._open[node] = _Open(self.clock() + backoff.next(), backoff)
        metrics.REDIS_BREAKER_TRIPS.inc()
        metrics.REDIS_BREAKER_OPEN.set(len(self._open))
        return True


class RateLimitedLog:
    """`warning(key, msg, *args)` logs at most once per `interval` seconds per key and appends how
    many identical events were suppressed since the last line."""

    def __init__(
        self, logger: logging.Logger, interval: float = 10.0, clock: Callable[[], float] = time.monotonic
    ):
        self.logger = logger
        self.interval = interval
        self.clock = clock
        self._last: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def _due(self, key: str) -> int | None:
        now = self.clock()
        if now - self._last.get(key, float("-inf")) < self.interval:
            self._suppressed[key] = self._suppressed.get(key, 0) + 1
            return None
        self._last[key] = now
        return self._suppressed.pop(key, 0)

    def log(self, level: int, key: str, msg: str, *args, **kwargs) -> bool:
        suppressed = self._due(key)
        if suppressed is None:
            return False
        if suppressed:
            msg += f" ({suppressed} similar suppressed in the last {self.interval:g}s)"
        self.logger.log(level, msg, *args, **kwargs)
        return True

    def warning(self, key: str, msg: str, *args, **kwargs) -> bool:
        return self.log(logging.WARNING, key, msg, *args, **kwargs)

    def info(self, key: str, msg: str, *args, **kwargs) -> bool:
        return self.log(logging.INFO, key, msg, *args, **kwargs)
