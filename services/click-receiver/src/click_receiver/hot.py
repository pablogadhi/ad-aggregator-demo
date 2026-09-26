"""Hot-ad detection (spec §5.1.1): batched counting, marking, and a cached in-process hot set.

Nothing here runs on the per-click path except `is_hot()` (a dict lookup) and `record()` (a dict
increment); the Redis work happens in two background loops.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime

from click_receiver import metrics
from click_receiver.settings import Settings
from click_receiver.store import ClickStore, HotAd

log = logging.getLogger("click_receiver.hot")


class HotTracker:
    def __init__(self, store: ClickStore, settings: Settings, clock: Callable[[], float] = time.time):
        self.store = store
        self.s = settings
        self.clock = clock
        self._pending: dict[int, int] = {}  # ad_id -> accepted clicks not yet flushed
        self._view: dict[int, HotAd] = {}  # last known hot set (kept while Redis is down)
        self.refreshed_at: datetime | None = None
        self._tasks: list[asyncio.Task] = []

    # -- per click (in-memory only) ----------------------------------------------------------
    def is_hot(self, ad_id: int) -> bool:
        return ad_id in self._view

    def salt_for(self, ad_id: int, hot: bool) -> int:
        """0 for normal ads; random 0..HOT_SALT_BUCKETS-1 for hot ads when salting is enabled."""
        if hot and self.s.hot_salting_enabled:
            return random.randrange(self.s.hot_salt_buckets)
        return 0

    def record(self, ad_id: int) -> None:
        """Count an accepted click (duplicates are never recorded)."""
        self._pending[ad_id] = self._pending.get(ad_id, 0) + 1

    @property
    def items(self) -> list[HotAd]:
        return sorted(self._view.values(), key=lambda h: h.ad_id)

    # -- background: flush counters + mark -------------------------------------------------
    def _requeue(self, counts: dict[int, int]) -> None:
        """Merge a failed batch back (bounded: beyond hot_pending_max_ads distinct ads, drop)."""
        for ad, n in counts.items():
            if ad in self._pending or len(self._pending) < self.s.hot_pending_max_ads:
                self._pending[ad] = self._pending.get(ad, 0) + n
            else:
                metrics.HOT_COUNTS_DROPPED.inc(n)

    async def flush(self) -> None:
        batch, self._pending = self._pending, {}
        if not batch:
            return
        now = self.clock()
        try:
            sums, failed = await self.store.add_counts(batch, int(now // 60))
        except Exception as exc:  # noqa: BLE001 — Redis down: keep counting locally
            metrics.HOT_FLUSH_ERRORS.inc()
            log.warning("hot counter flush failed (%d ads kept): %s", len(batch), exc)
            self._requeue(batch)
            return
        if failed:
            metrics.HOT_FLUSH_ERRORS.inc()
            self._requeue({ad: batch[ad] for ad in failed})
        over = sorted(ad for ad, total in sums.items() if total >= self.s.hot_threshold_clicks_10m)
        if not over:
            return
        try:
            marked = await self.store.mark(
                over, now, self.s.hot_mark_ttl_seconds, self.s.hot_permanent_after_marks
            )
        except Exception as exc:  # noqa: BLE001 — retried implicitly by the next flush of the ad
            metrics.HOT_FLUSH_ERRORS.inc()
            log.warning("hot marking failed: %s", exc)
            return
        until = datetime.fromtimestamp(now + self.s.hot_mark_ttl_seconds, UTC)
        for ad, m in marked.items():
            if m.new:
                metrics.HOT_MARKINGS.inc()
                log.info("ad marked hot", extra={"extra_fields": {"ad_id": ad, "marks": m.marks}})
            # our own marking is visible here at once; the refresher confirms it for everyone
            self._view[ad] = HotAd(ad, m.marks >= self.s.hot_permanent_after_marks, m.marks, until)
        self._update_gauges()

    # -- background: refresh the hot set ---------------------------------------------------
    async def refresh(self) -> None:
        try:
            view = await self.store.load_hot(self.clock(), self.s.hot_permanent_after_marks)
        except Exception as exc:  # noqa: BLE001 — keep the last known set (hot ads stay salted)
            metrics.HOT_REFRESH_ERRORS.inc()
            log.warning("hot set refresh failed, keeping %d known ads: %s", len(self._view), exc)
            return
        self._view = view
        self.refreshed_at = datetime.fromtimestamp(self.clock(), UTC)
        self._update_gauges()

    def _update_gauges(self) -> None:
        metrics.HOT_ADS.set(len(self._view))
        metrics.HOT_ADS_PERMANENT.set(sum(1 for h in self._view.values() if h.permanent))

    # -- lifecycle -------------------------------------------------------------------------
    async def _loop(self, fn, interval_ms: int) -> None:
        while True:
            await asyncio.sleep(interval_ms / 1000)
            try:
                await fn()
            except Exception:  # noqa: BLE001 — a background loop must never die
                log.exception("hot-ad background step failed")

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._loop(self.flush, self.s.hot_flush_interval_ms), name="hot-flush"),
            asyncio.create_task(self._loop(self.refresh, self.s.hot_refresh_interval_ms), name="hot-refresh"),
        ]
        # learn the current hot set right away instead of salting nothing for the first 2 s
        self._tasks.append(asyncio.create_task(self.refresh(), name="hot-refresh-initial"))

    async def stop(self, flush_timeout: float = 3.0) -> None:
        """Graceful shutdown: stop the loops, then flush the last second of counters."""
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.flush(), flush_timeout)
