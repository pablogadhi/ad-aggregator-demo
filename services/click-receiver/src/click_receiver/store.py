"""Redis access for click-receiver: the dedup key (hot path) and the hot-ad state (spec §5.1.1).

Works with `redis.asyncio.Redis` (standalone) and `redis.asyncio.cluster.RedisCluster`:
- every per-ad key carries the hash tag `{a:<id>}`, so the per-ad multi-key commands (MGET over
  the minute buckets, the mark script over flag+marks) stay inside one cluster slot;
- batches are *non-transactional* pipelines, which RedisCluster splits per node. MGET/EVAL are
  queued with `execute_command` because the cluster pipeline blocks the `.mget()`/`.eval()`
  helpers (they could span slots in general); ours never do, thanks to the hash tag.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

# KEYS[1] = hot:{a:ID}:flag, KEYS[2] = hot:{a:ID}:marks, ARGV[1] = flag TTL (s)
# -> {new_marking (1|0), marks}. Atomic, so two receivers crossing the threshold in the same
# second produce exactly one marking.
MARK_LUA = """
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  return {1, redis.call('INCR', KEYS[2])}
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return {0, tonumber(redis.call('GET', KEYS[2]) or '0')}
"""

HOT_ADS = "hot:ads"  # zset ad_id -> hot-until (epoch s)
HOT_PERM = "hot:perm"  # set of permanently hot ad ids
BUCKET_TTL = 660  # 11 minutes: a bucket outlives the 10-minute window it is summed in
WINDOW_MINUTES = 10


def dedup_key(ad_id: int, user_id: str) -> str:
    # No hash tag on purpose: dedup keys spread over all shards (a hot ad must not be a hot shard).
    return f"click:dedup:{ad_id}:{user_id}"


def bucket_key(ad_id: int, minute: int) -> str:
    return f"hot:{{a:{ad_id}}}:c:{minute}"


def flag_key(ad_id: int) -> str:
    return f"hot:{{a:{ad_id}}}:flag"


def marks_key(ad_id: int) -> str:
    return f"hot:{{a:{ad_id}}}:marks"


@dataclass(frozen=True, slots=True)
class HotAd:
    ad_id: int
    permanent: bool
    marks: int
    hot_until: datetime | None


@dataclass(frozen=True, slots=True)
class Marking:
    new: bool
    marks: int


class ClickStore(Protocol):
    async def claim(self, key: str, click_id: str, ttl: int) -> bool: ...
    async def release(self, key: str) -> None: ...
    async def add_counts(self, counts: dict[int, int], minute: int) -> tuple[dict[int, int], set[int]]: ...
    async def mark(
        self, ad_ids: list[int], now: float, ttl: int, permanent_after: int
    ) -> dict[int, Marking]: ...
    async def load_hot(self, now: float, permanent_after: int) -> dict[int, HotAd]: ...
    async def connect(self) -> None: ...


def _ok(result) -> bool:
    return not isinstance(result, BaseException)


class RedisClickStore:
    """`fast` serves the per-click dedup (tight socket timeout, no retries: the caller fails open);
    `bg` serves the batched hot-ad work (longer timeouts, retries), so slow background batches
    never queue in front of a click's SET NX.

    `refresher` (cluster mode only, see redis_cluster.py) is told about every per-click command's
    outcome so repeated failures on one node trigger a slot-map refresh."""

    def __init__(self, fast, bg, refresher=None):
        self.fast = fast
        self.bg = bg
        self.refresher = refresher

    # -- hot path: exactly one round trip per click ---------------------------------------
    async def claim(self, key: str, click_id: str, ttl: int) -> bool:
        if self.refresher is None:
            return bool(await self.fast.set(key, click_id, nx=True, ex=ttl))
        try:
            ok = bool(await self.fast.set(key, click_id, nx=True, ex=ttl))
        except BaseException:  # incl. the caller's safety timeout cancelling us
            self.refresher.failure(key)
            raise
        self.refresher.success(key)
        return ok

    async def release(self, key: str) -> None:
        await self.fast.delete(key)

    async def connect(self) -> None:
        """Warm-up: load the cluster slot map and open a connection to every primary (or PING
        the standalone server), so the first clicks don't pay for it."""
        for client in (self.fast, self.bg):
            if hasattr(client, "get_primaries"):  # RedisCluster
                await client.initialize()
                await client.ping(target_nodes=client.PRIMARIES)
            else:
                await client.ping()

    # -- batched (flusher, every ~1 s) -------------------------------------------------------
    async def add_counts(self, counts: dict[int, int], minute: int) -> tuple[dict[int, int], set[int]]:
        """INCRBY+EXPIRE the current minute bucket of each ad and read the 9 previous buckets, in
        one pipeline. Returns ({ad: 10-minute sum}, {ads whose INCRBY failed -> retry later})."""
        ads = list(counts)
        pipe = self.bg.pipeline(transaction=False)
        for ad in ads:
            cur = bucket_key(ad, minute)
            pipe.incrby(cur, counts[ad])
            pipe.expire(cur, BUCKET_TTL)
            pipe.execute_command("MGET", *(bucket_key(ad, minute - i) for i in range(1, WINDOW_MINUTES)))
        results = await pipe.execute(raise_on_error=False)
        sums: dict[int, int] = {}
        failed: set[int] = set()
        for i, ad in enumerate(ads):
            incr, _expire, prev = results[3 * i : 3 * i + 3]
            if not _ok(incr):
                failed.add(ad)
                continue
            total = int(incr)
            if _ok(prev):
                total += sum(int(v) for v in prev if v is not None)
            sums[ad] = total
        return sums, failed

    async def mark(self, ad_ids: list[int], now: float, ttl: int, permanent_after: int) -> dict[int, Marking]:
        """Run the mark script for each ad over the threshold, index it in `hot:ads`, and add it to
        `hot:perm` once it has been marked `permanent_after` times."""
        pipe = self.bg.pipeline(transaction=False)
        for ad in ad_ids:
            pipe.execute_command("EVAL", MARK_LUA, 2, flag_key(ad), marks_key(ad), ttl)
            pipe.zadd(HOT_ADS, {str(ad): now + ttl})
        results = await pipe.execute(raise_on_error=False)
        out: dict[int, Marking] = {}
        for i, ad in enumerate(ad_ids):
            res = results[2 * i]
            if _ok(res):
                out[ad] = Marking(new=bool(int(res[0])), marks=int(res[1]))
        perm = [ad for ad, m in out.items() if m.marks >= permanent_after]
        if perm:
            await self.bg.sadd(HOT_PERM, *map(str, perm))
        return out

    # -- cached read (refresher, every ~2 s) ------------------------------------------------
    async def load_hot(self, now: float, permanent_after: int) -> dict[int, HotAd]:
        pipe = self.bg.pipeline(transaction=False)
        pipe.zremrangebyscore(HOT_ADS, "-inf", f"({now}")
        pipe.zrangebyscore(HOT_ADS, now, "+inf", withscores=True)
        pipe.smembers(HOT_PERM)
        _, temp, perm = await pipe.execute()
        until = {int(member): float(score) for member, score in temp}
        perm_ids = {int(m) for m in perm}
        ids = sorted(until.keys() | perm_ids)
        marks: dict[int, int] = {}
        if ids:
            pipe = self.bg.pipeline(transaction=False)
            for ad in ids:
                pipe.get(marks_key(ad))
            marks = {ad: int(v or 0) for ad, v in zip(ids, await pipe.execute(), strict=True)}
        return {
            ad: HotAd(
                ad_id=ad,
                permanent=ad in perm_ids or marks[ad] >= permanent_after,
                marks=marks[ad],
                hot_until=datetime.fromtimestamp(until[ad], UTC) if ad in until else None,
            )
            for ad in ids
        }
