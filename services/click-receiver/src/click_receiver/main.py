"""click-receiver — implements design/contracts/openapi/click-receiver.yaml (spec §5.1, §5.1.1).

The hot path: ad lookup (cache) -> dedup (Redis SET NX, fail open) -> produce to Kafka `clicks`
(acks=all, idempotent) -> answer only after the ack. Hot-ad detection runs in the background.
"""

from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Path, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from sdl_common import create_app

from click_receiver.service import AdNotFound, ClickNotRecorded, ClickOutcome, ClickService
from click_receiver.settings import Settings

settings = Settings()


def producer_config(bootstrap_servers: str, security_protocol: str, s: Settings) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": security_protocol,
        "client.id": f"click-receiver-{s.pod_name}",
        # "no clicks lost": the ack means the record is on min.insync.replicas (2 of 3) brokers,
        # and idempotence makes librdkafka's internal retries (leader failover) duplicate-free.
        "acks": "all",
        "enable.idempotence": True,
        # bounds the whole send incl. retries; the gateway gives the request 2 s in total
        "delivery.timeout.ms": s.kafka_delivery_timeout_ms,
        # small batching window: concurrent clicks share a produce request without hurting p95
        "linger.ms": 5,
        "socket.keepalive.enable": True,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Imported here so unit tests (which never run the lifespan) need no connection env vars.
    from redis.asyncio.retry import Retry
    from redis.backoff import NoBackoff
    from sdl_common.kafka import AsyncProducer, KafkaSettings
    from sdl_common.postgres import Database, PostgresSettings
    from sdl_common.redis import RedisSettings, connect

    from click_receiver.ads import AdCache, PostgresAds
    from click_receiver.hot import HotTracker
    from click_receiver.redis_cluster import FastRedisCluster, SlotMapRefresher
    from click_receiver.store import RedisClickStore
    from click_receiver.warmup import Warmup

    s = settings
    ks, rs = KafkaSettings(), RedisSettings()
    async with Database(PostgresSettings()) as db:
        repo = PostgresAds(db, timeout=s.db_timeout_ms / 1000)
        ads = AdCache(repo, s.ad_cache_ttl_seconds, s.ad_cache_max_entries)
        budget = s.redis_timeout_ms / 1000
        # Per-click client: no retries, and REDIS_TIMEOUT_MS bounds the socket I/O (connect and
        # each read), not the wall-clock time incl. event-loop queueing — see service.py.
        fast_kwargs = dict(
            socket_timeout=budget,
            socket_connect_timeout=budget,
            retry=Retry(NoBackoff(), 0),
            decode_responses=True,
        )
        if rs.mode == "cluster":
            fast = FastRedisCluster.from_url(rs.url, **fast_kwargs)
        else:
            fast = connect(rs, **fast_kwargs)
        refresher = SlotMapRefresher.for_client(
            fast,
            threshold=s.redis_slot_refresh_after_failures,
            min_interval=s.redis_slot_refresh_min_interval_ms / 1000,
            interval=s.redis_slot_refresh_interval_ms / 1000,
        )
        bg = connect(rs, socket_timeout=1.0, socket_connect_timeout=1.0)
        store = RedisClickStore(fast, bg, refresher)
        producer = AsyncProducer(producer_config(ks.bootstrap_servers, ks.security_protocol, s)).start()
        hot = HotTracker(store, s)
        app.state.service = ClickService(s, ads, store, producer, hot)

        step = s.warmup_step_timeout_ms / 1000
        redis_step = s.warmup_redis_timeout_ms / 1000

        async def preload_ads() -> str:
            return f"{ads.preload(await repo.preload(min(s.ad_preload_limit, s.ad_cache_max_entries)))} ads"

        async def load_hot() -> str:
            await hot.refresh()
            return f"{len(hot.items)} hot ads"

        warmup = Warmup(
            [
                ("kafka", lambda: producer.check(s.clicks_topic, metadata_timeout=step), step + 0.5, 3),
                ("redis", store.connect, redis_step, 1),
                ("ad_cache", preload_ads, step, 3),
                ("hot_set", load_hot, redis_step, 1),
            ]
        )
        app.state.checks = [
            ("warmup", warmup.check),
            ("kafka", lambda: producer.check(s.clicks_topic)),
            ("postgres", repo.check),
            # Redis is deliberately not a readiness dependency: the click path fails open.
        ]
        warmup.start()
        hot.start()
        if refresher is not None:
            refresher.start()
        try:
            yield
        finally:
            # uvicorn has drained in-flight requests by now: flush counters, then the producer
            await warmup.stop()
            if refresher is not None:
                await refresher.stop()
            await hot.stop()
            await producer.close(flush_timeout=10)
            for client in (fast, bg):
                try:
                    await client.aclose()
                except Exception:  # noqa: BLE001, S110
                    pass


def readiness():
    return getattr(app.state, "checks", [])


# Access log sampled (ACCESS_LOG_SAMPLE, default 1 %): a JSON line per click was a measurable share
# of the per-click CPU at thousands of clicks/s; 4xx/5xx are always logged.
app = create_app(
    settings,
    title="click-receiver",
    lifespan=lifespan,
    readiness=readiness,
    access_log_sample=settings.access_log_sample,
)


# async on purpose: FastAPI runs a sync dependency in the thread pool (a thread hop per click)
async def get_service(request: Request) -> ClickService:
    return request.app.state.service


Service = Annotated[ClickService, Depends(get_service)]


# -- schemas (contract components) ------------------------------------------------------------
class Error(BaseModel):
    detail: str


class ClickRequest(BaseModel):
    ad_id: int = Field(ge=1)
    user_id: str = Field(min_length=1, max_length=128)


class ClickResult(BaseModel):
    click_id: uuid.UUID
    status: Literal["accepted", "duplicate"]
    ad_id: int
    redirect_url: str
    clicked_at: datetime
    hot: bool


class HotAdOut(BaseModel):
    ad_id: int
    permanent: bool
    marks: int = Field(ge=0)
    hot_until: datetime | None


class HotAds(BaseModel):
    refreshed_at: datetime | None
    threshold_clicks_10m: int
    salt_buckets: int
    salting_enabled: bool
    items: list[HotAdOut]


ERRORS = {
    404: {"model": Error, "description": "Unknown or inactive ad; nothing recorded"},
    503: {"model": Error, "description": "Click could not be durably recorded; the client may retry"},
}


def click_headers(outcome: ClickOutcome) -> dict[str, str]:
    return {
        "X-Click-Status": outcome.status,
        "X-Click-Id": outcome.click_id,
        "X-Click-Hot": "true" if outcome.hot else "false",
    }


async def run_click(
    service: ClickService, ad_id: int, user_id: str, started: float
) -> ClickOutcome | JSONResponse:
    click_id = str(uuid.uuid4())
    try:
        return await service.handle(ad_id, user_id, click_id, started)
    except AdNotFound as exc:
        return JSONResponse({"detail": str(exc)}, status_code=404, headers={"X-Click-Id": click_id})
    except ClickNotRecorded as exc:
        return JSONResponse({"detail": str(exc)}, status_code=503, headers={"X-Click-Id": click_id})


# -- routes ----------------------------------------------------------------------------------
@app.get(
    "/click/{ad_id}",
    status_code=302,
    response_class=RedirectResponse,
    responses={302: {"description": "Accepted or duplicate; redirect to the advertiser"}, **ERRORS},
    operation_id="clickRedirect",
)
async def click_redirect(
    service: Service,
    ad_id: Annotated[int, Path(ge=1)],
    user_id: Annotated[str, Query(min_length=1, max_length=128)],
):
    started = time.monotonic()  # CLICK_DEADLINE_MS is measured from here
    outcome = await run_click(service, ad_id, user_id, started)
    if isinstance(outcome, JSONResponse):
        return outcome
    return RedirectResponse(outcome.ad.redirect_url, status_code=302, headers=click_headers(outcome))


@app.post("/clicks", response_model=ClickResult, responses=ERRORS, operation_id="recordClick")
async def record_click(service: Service, body: ClickRequest):
    started = time.monotonic()  # CLICK_DEADLINE_MS is measured from here
    outcome = await run_click(service, body.ad_id, body.user_id, started)
    if isinstance(outcome, JSONResponse):
        return outcome
    result = ClickResult(
        click_id=outcome.click_id,
        status=outcome.status,
        ad_id=outcome.ad.id,
        redirect_url=outcome.ad.redirect_url,
        clicked_at=outcome.clicked_at,
        hot=outcome.hot,
    )
    return JSONResponse(result.model_dump(mode="json"), headers=click_headers(outcome))


@app.get("/hot-ads", response_model=HotAds, operation_id="listHotAds")
async def list_hot_ads(service: Service) -> HotAds:
    hot, s = service.hot, service.s
    return HotAds(
        refreshed_at=hot.refreshed_at,
        threshold_clicks_10m=s.hot_threshold_clicks_10m,
        salt_buckets=s.hot_salt_buckets,
        salting_enabled=s.hot_salting_enabled,
        items=[
            HotAdOut(ad_id=h.ad_id, permanent=h.permanent, marks=h.marks, hot_until=h.hot_until)
            for h in hot.items
        ],
    )
