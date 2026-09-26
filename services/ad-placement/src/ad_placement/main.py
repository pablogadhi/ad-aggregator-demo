"""ad-placement — implements design/contracts/openapi/ad-placement.yaml (spec §5, §5.3).

Advertiser + ad CRUD (owner-scoped) and active-ad reads for display. JWT is verified at the gateway;
this service only authorizes from the X-Auth-* claim headers (sdl_common.auth):
- any authenticated role: GET /ads, GET /ads/{ad_id}, POST /advertisers
- owner only (role advertiser + X-Auth-Advertiser-Id == path id): /advertisers/{advertiser_id}/...
Order of checks: 401 (no identity) -> 403 (not owner) -> 404 (missing advertiser/ad).

Connection `postgres` (POSTGRES_URL primary, POSTGRES_READ_URL replicas). Schema migrated by the
init container: `python -m sdl_common.migrate ad_placement.migrations`.
"""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response, status
from pydantic import AfterValidator, BaseModel, Field
from sdl_common import ServiceSettings, create_app
from sdl_common.auth import FORBIDDEN, UNAUTHORIZED, Identity, advertiser_owner, current_identity
from sdl_common.pgread import ReadRouter, install_db_error_handlers
from sdl_common.postgres import Database, PostgresSettings

from ad_placement.repo import AdRepo, PgAdRepo


class Settings(ServiceSettings):
    click_url_prefix: str = "/api/click-receiver/click"


settings = Settings(service_name="ad-placement")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with Database(PostgresSettings()) as db:
        app.state.db = db
        app.state.repo = PgAdRepo(db, ReadRouter(db))
        yield


def readiness():
    # the primary: every owner endpoint and every write needs it
    return [("postgres", app.state.db.check)]


app = create_app(settings, title="ad-placement", lifespan=lifespan, readiness=readiness)
install_db_error_handlers(app)  # DB down / pool exhausted -> 503 {"detail"} instead of 500


def get_repo(request: Request) -> AdRepo:
    return request.app.state.repo


Repo = Annotated[AdRepo, Depends(get_repo)]
AnyRole = Annotated[Identity, Depends(current_identity)]
Owner = Annotated[Identity, Depends(advertiser_owner)]
AdvertiserId = Annotated[int, Path(ge=1)]
AdId = Annotated[int, Path(ge=1)]

# ---- schemas (contract: components.schemas) ----


def _absolute_http_url(value: str) -> str:
    # Kept verbatim (no normalisation): the click-receiver redirects to exactly what was stored.
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError("must be an absolute http(s) URL")
    return value


def _absolute_uri(value: str | None) -> str | None:
    if value is not None and not urlsplit(value).scheme:
        raise ValueError("must be an absolute URI")
    return value


HttpUrlStr = Annotated[str, AfterValidator(_absolute_http_url)]
UriStr = Annotated[str | None, AfterValidator(_absolute_uri)]


class Error(BaseModel):
    detail: str


NOT_FOUND = {404: {"description": "No such advertiser/ad", "model": Error}}


class AdvertiserCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class Advertiser(BaseModel):
    id: int
    name: str
    created_at: datetime


class AdCreate(BaseModel):
    content: str = Field(min_length=1, max_length=2000)
    img_url: UriStr = None
    redirect_url: HttpUrlStr


class AdUpdate(BaseModel):
    content: str = Field(min_length=1, max_length=2000)
    img_url: UriStr = None
    redirect_url: HttpUrlStr
    active: bool


class Ad(BaseModel):
    id: int
    advertiser_id: int
    content: str
    img_url: str | None
    redirect_url: str
    active: bool
    click_url: str
    created_at: datetime
    updated_at: datetime


class AdPage(BaseModel):
    items: list[Ad]
    next_after_id: int | None


class AdList(BaseModel):
    items: list[Ad]


def to_ad(row: dict[str, Any]) -> Ad:
    return Ad(**row, click_url=f"{settings.click_url_prefix.rstrip('/')}/{row['id']}")


def _not_found(what: str) -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, detail=f"{what} not found")


# ---- display reads (any role) ----


@app.get("/ads", response_model=AdPage, responses=UNAUTHORIZED)
async def list_active_ads(
    who: AnyRole,
    repo: Repo,
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
    after_id: Annotated[int, Query(ge=0)] = 0,
) -> AdPage:
    # Keyset pagination: fetch one extra row to know whether another page exists (stable under
    # concurrent inserts, O(limit) per page unlike OFFSET).
    rows = await repo.list_active(limit=limit + 1, after_id=after_id)
    page = rows[:limit]
    return AdPage(items=[to_ad(r) for r in page], next_after_id=page[-1]["id"] if len(rows) > limit else None)


@app.get("/ads/{ad_id}", response_model=Ad, responses={**UNAUTHORIZED, **NOT_FOUND})
async def get_ad(ad_id: AdId, who: AnyRole, repo: Repo) -> Ad:
    row = await repo.get_active(ad_id)
    if row is None:
        raise _not_found("ad")
    return to_ad(row)


@app.post("/advertisers", response_model=Advertiser, status_code=201, responses=UNAUTHORIZED)
async def create_advertiser(body: AdvertiserCreate, who: AnyRole, repo: Repo) -> Advertiser:
    return Advertiser(**await repo.create_advertiser(body.name))


# ---- owner-scoped ----

OWNER_RESPONSES = {**UNAUTHORIZED, **FORBIDDEN, **NOT_FOUND}


@app.get("/advertisers/{advertiser_id}", response_model=Advertiser, responses=OWNER_RESPONSES)
async def get_advertiser(advertiser_id: AdvertiserId, who: Owner, repo: Repo) -> Advertiser:
    row = await repo.get_advertiser(advertiser_id)
    if row is None:
        raise _not_found("advertiser")
    return Advertiser(**row)


@app.get("/advertisers/{advertiser_id}/ads", response_model=AdList, responses=OWNER_RESPONSES)
async def list_advertiser_ads(
    advertiser_id: AdvertiserId, who: Owner, repo: Repo, include_inactive: bool = True
) -> AdList:
    rows = await repo.list_advertiser_ads(advertiser_id, include_inactive=include_inactive)
    if rows is None:
        raise _not_found("advertiser")
    return AdList(items=[to_ad(r) for r in rows])


@app.post("/advertisers/{advertiser_id}/ads", response_model=Ad, status_code=201, responses=OWNER_RESPONSES)
async def create_ad(advertiser_id: AdvertiserId, body: AdCreate, who: Owner, repo: Repo) -> Ad:
    row = await repo.create_ad(
        advertiser_id, content=body.content, img_url=body.img_url, redirect_url=body.redirect_url
    )
    if row is None:  # the token is for an advertiser id that was never created (auth doesn't check)
        raise _not_found("advertiser")
    return to_ad(row)


@app.put("/advertisers/{advertiser_id}/ads/{ad_id}", response_model=Ad, responses=OWNER_RESPONSES)
async def update_ad(advertiser_id: AdvertiserId, ad_id: AdId, body: AdUpdate, who: Owner, repo: Repo) -> Ad:
    row = await repo.update_ad(
        advertiser_id,
        ad_id,
        content=body.content,
        img_url=body.img_url,
        redirect_url=body.redirect_url,
        active=body.active,
    )
    if row is None:  # no such ad, or it belongs to another advertiser (don't leak which)
        raise _not_found("ad")
    return to_ad(row)


@app.delete("/advertisers/{advertiser_id}/ads/{ad_id}", status_code=204, responses=OWNER_RESPONSES)
async def deactivate_ad(advertiser_id: AdvertiserId, ad_id: AdId, who: Owner, repo: Repo) -> Response:
    # Soft delete (active = false), idempotent. The click-receiver caches ads for up to 60 s, so
    # clicks may still be accepted briefly afterwards (spec §5.3, documented).
    if not await repo.deactivate_ad(advertiser_id, ad_id):
        raise _not_found("ad")
    return Response(status_code=204)
