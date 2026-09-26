"""analytics — implements design/contracts/openapi/analytics.yaml (spec §5, §5.4).

Click metrics for advertisers from `click_counts` (analytics-db), per ad and per advertiser, rolled up
to minute / hour / day UTC buckets (bucket math in analytics.buckets). JWT is verified at the
gateway; this service authorizes from the X-Auth-* headers: role advertiser and
X-Auth-Advertiser-Id == path advertiser_id, else 403; no identity -> 401.

Connection `analytics-db` (ANALYTICS_DB_URL primary, ANALYTICS_DB_READ_URL replicas). Reads go to
the replica and fall back to the primary (sdl_common.pgread.ReadRouter). Schema migrated by the init
container: `python -m sdl_common.migrate analytics.migrations ANALYTICS_DB_URL`.
"""

from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sdl_common import ServiceSettings, create_app
from sdl_common.auth import FORBIDDEN, UNAUTHORIZED, Identity, advertiser_owner
from sdl_common.pgread import ReadRouter, install_db_error_handlers, postgres_settings
from sdl_common.postgres import Database

from analytics.buckets import BucketRange, RangeError, resolve_range
from analytics.repo import ClickRepo, PgClickRepo, Series

DB_ENV_PREFIX = "ANALYTICS_DB_"  # connection `analytics-db` -> ANALYTICS_DB_* env vars


class Settings(ServiceSettings):
    max_buckets: int = Field(default=1440, ge=1)


settings = Settings(service_name="analytics")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with Database(postgres_settings(DB_ENV_PREFIX)) as db:
        app.state.reads = ReadRouter(db)
        app.state.repo = PgClickRepo(app.state.reads)
        yield


def readiness():
    # ready while reads can be served: replica, or the primary as fallback
    return [("analytics-db", app.state.reads.check)]


app = create_app(settings, title="analytics", lifespan=lifespan, readiness=readiness)
install_db_error_handlers(app)  # DB unavailable -> 503 {"detail"}


def get_repo(request: Request) -> ClickRepo:
    return request.app.state.repo


def get_clock() -> Callable[[], datetime]:
    return lambda: datetime.now(UTC)


# ---- schemas (contract: components.schemas) ----


class Error(BaseModel):
    detail: str


class Point(BaseModel):
    start: datetime
    clicks: int


class Freshness(BaseModel):
    last_updated_at: datetime | None


class AdTotal(BaseModel):
    ad_id: int
    clicks: int


class _Range(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    granularity: Literal["minute", "hour", "day"]
    from_: datetime = Field(alias="from", description="Effective (truncated) start")
    to: datetime = Field(description="Effective (rounded up) exclusive end")
    total: int
    points: list[Point]
    freshness: Freshness


class AdClicks(_Range):
    advertiser_id: int
    ad_id: int


class AdvertiserClicks(_Range):
    advertiser_id: int
    by_ad: list[AdTotal]


# ---- parameters ----

Owner = Annotated[Identity, Depends(advertiser_owner)]
Repo = Annotated[ClickRepo, Depends(get_repo)]
Clock = Annotated[Callable[[], datetime], Depends(get_clock)]
AdvertiserId = Annotated[int, Path(ge=1)]
# AwareDatetime: timestamps without a UTC offset are rejected with 422 (contract)
From = Annotated[
    AwareDatetime | None, Query(alias="from", description="RFC 3339 timestamp (default to - 1h)")
]
To = Annotated[AwareDatetime | None, Query(description="RFC 3339 timestamp, exclusive (default now)")]
GranularityQ = Annotated[Literal["minute", "hour", "day"], Query()]

RESPONSES = {
    400: {"description": "from >= to, or too many buckets", "model": Error},
    **UNAUTHORIZED,
    **FORBIDDEN,
}


def _range(
    from_: datetime | None, to: datetime | None, granularity: str, clock: Callable[[], datetime]
) -> BucketRange:
    try:
        return resolve_range(from_, to, granularity, now=clock(), max_buckets=settings.max_buckets)
    except RangeError as exc:
        raise HTTPException(400, detail=str(exc)) from exc


def _series_fields(rng: BucketRange, series: Series) -> dict:
    points = [Point(start=start, clicks=clicks) for start, clicks in rng.zero_fill(series.buckets)]
    return {
        "granularity": rng.granularity,
        "from_": rng.start,
        "to": rng.end,
        "total": sum(p.clicks for p in points),
        "points": points,
        "freshness": Freshness(last_updated_at=series.last_updated_at),
    }


# ---- routes ----


@app.get("/advertisers/{advertiser_id}/clicks", response_model=AdvertiserClicks, responses=RESPONSES)
async def get_advertiser_clicks(
    advertiser_id: AdvertiserId,
    who: Owner,
    repo: Repo,
    clock: Clock,
    from_: From = None,
    to: To = None,
    granularity: GranularityQ = "minute",
) -> AdvertiserClicks:
    rng = _range(from_, to, granularity, clock)
    series = await repo.advertiser_series(advertiser_id, rng)
    by_ad = sorted(
        (AdTotal(ad_id=ad, clicks=c) for ad, c in series.by_ad.items() if c > 0),
        key=lambda t: (-t.clicks, t.ad_id),  # clicks descending; ad_id breaks ties deterministically
    )
    return AdvertiserClicks(advertiser_id=advertiser_id, by_ad=by_ad, **_series_fields(rng, series))


@app.get("/advertisers/{advertiser_id}/ads/{ad_id}/clicks", response_model=AdClicks, responses=RESPONSES)
async def get_ad_clicks(
    advertiser_id: AdvertiserId,
    ad_id: Annotated[int, Path(ge=1)],
    who: Owner,
    repo: Repo,
    clock: Clock,
    from_: From = None,
    to: To = None,
    granularity: GranularityQ = "minute",
) -> AdClicks:
    rng = _range(from_, to, granularity, clock)
    series = await repo.ad_series(advertiser_id, ad_id, rng)
    return AdClicks(advertiser_id=advertiser_id, ad_id=ad_id, **_series_fields(rng, series))
