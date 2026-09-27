"""Warm-up before readiness (spec §5.1 step 6).

A pod added by the HPA in the middle of a burst gets traffic as soon as /readyz is green; if its
first requests pay for the Kafka metadata, the Redis slot map + connections, one DB query per ad
and an empty hot set (unsalted hot ads), the burst's p95 pays for it. So the lifespan starts this
in the background (the lifespan itself must not block: /healthz has to answer meanwhile) and
`check` keeps /readyz red until it has finished:

1. producer: fetch metadata for `clicks` (connects to the cluster)       up to 3 attempts
2. Redis: load the slot map, connect to every primary                   1 attempt, short timeout
3. ad cache: preload the newest active ads (bounded, replica)           up to 3 attempts
4. hot set: load it once                                                 1 attempt (with 2.)

Every step is bounded and a failed step is skipped, never fatal: Redis is not a readiness
dependency (the click path fails open), and Kafka/Postgres are still gated by their own readiness
checks. Readiness semantics are otherwise unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from click_receiver import metrics

log = logging.getLogger("click_receiver.warmup")


class Warmup:
    def __init__(self, steps: list[tuple[str, Callable[[], Awaitable[object]], float, int]]):
        """steps: (name, async fn, timeout per attempt in s, attempts)."""
        self.steps = steps
        self.done = asyncio.Event()
        self.results: dict[str, str] = {}
        self._task: asyncio.Task | None = None

    async def run(self) -> dict[str, str]:
        start = time.monotonic()
        for name, fn, timeout, attempts in self.steps:
            t0 = time.monotonic()
            for attempt in range(1, attempts + 1):
                try:
                    result = await asyncio.wait_for(fn(), timeout)
                except Exception as exc:  # noqa: BLE001 — a warm-up step never blocks readiness forever
                    self.results[name] = f"skipped: {type(exc).__name__}: {exc}"[:200]
                    if attempt < attempts:
                        await asyncio.sleep(min(0.5 * attempt, 2.0))
                    continue
                ms = round((time.monotonic() - t0) * 1000)
                self.results[name] = f"ok ({result}, {ms} ms)" if result is not None else f"ok ({ms} ms)"
                break
        elapsed = time.monotonic() - start
        metrics.WARMUP_SECONDS.set(elapsed)
        log.info("warm-up done", extra={"extra_fields": {"seconds": round(elapsed, 3), **self.results}})
        self.done.set()
        return self.results

    def start(self) -> None:
        self._task = asyncio.create_task(self.run(), name="warmup")

    async def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except BaseException:  # noqa: BLE001, S110 — cancelled on shutdown
                pass

    async def check(self) -> None:
        """Readiness check: fails until the warm-up has finished."""
        if not self.done.is_set():
            raise RuntimeError("warming up")
