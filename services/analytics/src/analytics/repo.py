"""Reads of `click_counts` (schema: migrations/0001_click_counts.sql = contracts/db/analytics-db.sql).

Rows are written only by the click-aggregator Flink job (one row per ad and UTC minute, absolute
counts). Queries roll minutes up with `date_trunc(granularity, minute, 'UTC')` — the 3-argument
form so the result doesn't depend on the session TimeZone — and run on the replica through
ReadRouter (primary if the replica pool is unavailable). Replica lag counts toward the 60 s
freshness budget and is visible to clients through `last_updated_at`.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

import psycopg
from sdl_common.pgread import ReadRouter

from analytics.buckets import BucketRange

# Bounded work per request (p95 target 500 ms): a runaway query is cancelled instead of piling up.
STATEMENT_TIMEOUT = "3s"


@dataclass
class Series:
    buckets: dict[datetime, int] = field(default_factory=dict)  # bucket start -> clicks (non-zero only)
    by_ad: dict[int, int] = field(default_factory=dict)  # ad_id -> clicks (advertiser query only)
    last_updated_at: datetime | None = None


class ClickRepo(Protocol):
    async def ad_series(self, advertiser_id: int, ad_id: int, rng: BucketRange) -> Series: ...
    async def advertiser_series(self, advertiser_id: int, rng: BucketRange) -> Series: ...


class PgClickRepo:
    def __init__(self, reads: ReadRouter):
        self.reads = reads

    async def ad_series(self, advertiser_id: int, ad_id: int, rng: BucketRange) -> Series:
        # advertiser_id is part of the filter: another advertiser's ad reads as an empty series
        # (analytics doesn't know the ads catalogue, so it's not a 404 — see the contract).
        async def q(conn: psycopg.AsyncConnection) -> Series:
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            cur = await conn.execute(
                "SELECT date_trunc(%(g)s, minute, 'UTC') AS bucket, sum(click_count)::bigint AS clicks, "
                "       max(updated_at) AS last_updated_at "
                "FROM click_counts "
                "WHERE ad_id = %(ad)s AND advertiser_id = %(adv)s "
                "  AND minute >= %(start)s AND minute < %(end)s "
                "GROUP BY 1",
                {"g": rng.granularity, "ad": ad_id, "adv": advertiser_id, "start": rng.start, "end": rng.end},
            )
            series = Series()
            for row in await cur.fetchall():
                series.buckets[row["bucket"]] = row["clicks"]
                series.last_updated_at = _max(series.last_updated_at, row["last_updated_at"])
            return series

        return await self.reads.run(q)

    async def advertiser_series(self, advertiser_id: int, rng: BucketRange) -> Series:
        # One index range scan on (advertiser_id, minute) for both the time series and the per-ad
        # totals: GROUPING SETS yields bucket rows (ad_id NULL) and per-ad rows (bucket NULL).
        async def q(conn: psycopg.AsyncConnection) -> Series:
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            cur = await conn.execute(
                "SELECT bucket, ad_id, sum(click_count)::bigint AS clicks, "
                "       max(updated_at) AS last_updated_at "
                "FROM (SELECT date_trunc(%(g)s, minute, 'UTC') AS bucket, ad_id, click_count, updated_at "
                "      FROM click_counts "
                "      WHERE advertiser_id = %(adv)s AND minute >= %(start)s AND minute < %(end)s) r "
                "GROUP BY GROUPING SETS ((bucket), (ad_id))",
                {"g": rng.granularity, "adv": advertiser_id, "start": rng.start, "end": rng.end},
            )
            series = Series()
            for row in await cur.fetchall():
                if row["ad_id"] is None:
                    series.buckets[row["bucket"]] = row["clicks"]
                    # every row in range is in exactly one bucket group: max over them is the freshness
                    series.last_updated_at = _max(series.last_updated_at, row["last_updated_at"])
                else:
                    series.by_ad[row["ad_id"]] = row["clicks"]
            return series

        return await self.reads.run(q)


def _max(a: datetime | None, b: datetime | None) -> datetime | None:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)
