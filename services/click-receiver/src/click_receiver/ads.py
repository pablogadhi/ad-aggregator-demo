"""Ad lookup: in-process cache (TTL 60 s) -> read replica -> primary on a miss (spec §5.1.2)."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Ad:
    id: int
    advertiser_id: int
    redirect_url: str
    active: bool


class AdLookupError(Exception):
    """Neither DB pool could answer (the click can't be handled -> 503)."""


class AdSource(Protocol):
    async def get(self, ad_id: int) -> Ad | None: ...


QUERY = "SELECT id, advertiser_id, redirect_url, active FROM ads WHERE id = %s"
# warm-up: the newest active ads (bounded) — one query on the read replica
PRELOAD_QUERY = (
    "SELECT id, advertiser_id, redirect_url, active FROM ads WHERE active ORDER BY id DESC LIMIT %s"
)


class PostgresAds:
    """Replica first; a miss (or a replica error) is retried on the primary, so an ad created a
    moment ago is not a 404 because of replication lag."""

    def __init__(self, db, timeout: float):
        self.db = db  # sdl_common.postgres.Database
        self.timeout = timeout

    async def _fetch(self, pool, ad_id: int) -> Ad | None:
        async def run() -> Ad | None:
            async with pool.connection(timeout=self.timeout) as conn:
                cur = await conn.execute(QUERY, (ad_id,))
                row = await cur.fetchone()
            return Ad(row["id"], row["advertiser_id"], row["redirect_url"], row["active"]) if row else None

        return await asyncio.wait_for(run(), self.timeout)

    async def get(self, ad_id: int) -> Ad | None:
        primary, replica = self.db.primary, self.db.replica
        if replica is not primary:
            try:
                ad = await self._fetch(replica, ad_id)
                if ad is not None:
                    return ad
            except Exception:  # noqa: BLE001 — replica down/slow: the primary decides
                pass
        try:
            return await self._fetch(primary, ad_id)
        except Exception as exc:
            raise AdLookupError(f"ad lookup failed: {type(exc).__name__}: {exc}") from exc

    async def preload(self, limit: int) -> list[Ad]:
        """Warm-up: up to `limit` active ads, from the replica (the primary if the replica fails)."""
        last: Exception | None = None
        for pool in {id(p): p for p in (self.db.replica, self.db.primary)}.values():
            try:
                async with pool.connection(timeout=self.timeout) as conn:
                    cur = await conn.execute(PRELOAD_QUERY, (limit,))
                    rows = await cur.fetchall()
                return [Ad(r["id"], r["advertiser_id"], r["redirect_url"], r["active"]) for r in rows]
            except Exception as exc:  # noqa: BLE001
                last = exc
        raise AdLookupError(f"ad preload failed: {type(last).__name__}: {last}") from last

    async def check(self) -> None:
        """Readiness: *some* pool answers. Checking only the primary would take every receiver out
        of rotation during a primary failover, although cached ads + the replicas still work."""
        errors = []
        for pool in {id(p): p for p in (self.db.replica, self.db.primary)}.values():
            try:
                async with pool.connection(timeout=1.5) as conn:
                    await conn.execute("SELECT 1")
                return
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{type(exc).__name__}: {exc}")
        raise RuntimeError("; ".join(errors))


class AdCache:
    """TTL cache in front of an AdSource with request coalescing: concurrent misses for the same ad
    share one DB query (a cold cache under load would otherwise stampede the replica).
    Only found rows are cached (an inactive ad is cached as inactive); unknown ids are not, so an
    ad created after a 404 becomes clickable immediately."""

    def __init__(
        self,
        source: AdSource,
        ttl: float,
        max_entries: int,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.source = source
        self.ttl = ttl
        self.max_entries = max_entries
        self.clock = clock
        self._entries: dict[int, tuple[float, Ad]] = {}
        self._inflight: dict[int, asyncio.Future] = {}

    def __len__(self) -> int:
        return len(self._entries)

    def preload(self, ads: list[Ad]) -> int:
        """Warm-up: fill the cache (bounded by max_entries). Each entry gets a TTL spread over
        [ttl/2, ttl] so the preloaded set doesn't expire in one instant and stampede the replica."""
        now = self.clock()
        n = 0
        for ad in ads:
            if len(self._entries) >= self.max_entries:
                break
            self._entries[ad.id] = (now + self.ttl * random.uniform(0.5, 1.0), ad)
            n += 1
        return n

    def cached(self, ad_id: int) -> Ad | None:
        """Synchronous fast path: the cached ad if present and fresh, else None (call get())."""
        entry = self._entries.get(ad_id)
        if entry is not None and entry[0] > self.clock():
            return entry[1]
        return None

    async def get(self, ad_id: int) -> Ad | None:
        entry = self._entries.get(ad_id)
        if entry is not None:
            if entry[0] > self.clock():
                return entry[1]
            del self._entries[ad_id]
        task = self._inflight.get(ad_id)
        if task is None:
            task = asyncio.ensure_future(self._load(ad_id))
            self._inflight[ad_id] = task
        # shield: a client disconnect cancels this request, not the shared lookup
        return await asyncio.shield(task)

    async def _load(self, ad_id: int) -> Ad | None:
        try:
            ad = await self.source.get(ad_id)
            if ad is not None:
                if len(self._entries) >= self.max_entries:
                    self._entries.pop(next(iter(self._entries)))  # oldest insertion
                self._entries[ad_id] = (self.clock() + self.ttl, ad)
            return ad
        finally:
            self._inflight.pop(ad_id, None)
