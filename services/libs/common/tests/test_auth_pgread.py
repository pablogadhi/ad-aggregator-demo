from contextlib import asynccontextmanager
from typing import Annotated

import psycopg
import pytest
from fastapi import Depends
from fastapi.testclient import TestClient
from psycopg_pool import PoolTimeout
from sdl_common import ServiceSettings, create_app
from sdl_common.auth import Identity, advertiser_owner, current_identity
from sdl_common.pgread import ReadRouter, install_db_error_handlers, postgres_settings

# ---- auth ----


def auth_app():
    app = create_app(ServiceSettings(service_name="t"))

    @app.get("/me")
    async def me(who: Annotated[Identity, Depends(current_identity)]) -> dict:
        return {"sub": who.sub, "role": who.role, "advertiser_id": who.advertiser_id}

    @app.get("/advertisers/{advertiser_id}")
    async def mine(advertiser_id: int, who: Annotated[Identity, Depends(advertiser_owner)]) -> dict:
        return {"ok": advertiser_id}

    return TestClient(app)


ADV5 = {"X-Auth-Sub": "advertiser:5", "X-Auth-Role": "advertiser", "X-Auth-Advertiser-Id": "5"}


@pytest.mark.parametrize(
    "headers",
    [{}, {"X-Auth-Role": "viewer"}, {"X-Auth-Sub": "u"}, {"X-Auth-Sub": " ", "X-Auth-Role": "viewer"}],
)
def test_missing_identity_is_401(headers):
    res = auth_app().get("/me", headers=headers)
    assert res.status_code == 401
    assert res.headers["WWW-Authenticate"] == "Bearer"


def test_identity_parsing():
    c = auth_app()
    assert c.get("/me", headers=ADV5).json() == {
        "sub": "advertiser:5",
        "role": "advertiser",
        "advertiser_id": 5,
    }
    viewer = {"X-Auth-Sub": "u1", "X-Auth-Role": "viewer", "X-Auth-Advertiser-Id": ""}
    assert c.get("/me", headers=viewer).json()["advertiser_id"] is None
    assert c.get("/me", headers={**viewer, "X-Auth-Advertiser-Id": "x"}).json()["advertiser_id"] is None


def test_owner_rules():
    c = auth_app()
    assert c.get("/advertisers/5", headers=ADV5).status_code == 200
    assert c.get("/advertisers/6", headers=ADV5).status_code == 403
    assert c.get("/advertisers/5").status_code == 401
    as_viewer = {**ADV5, "X-Auth-Role": "viewer"}
    assert c.get("/advertisers/5", headers=as_viewer).status_code == 403
    assert c.get("/advertisers/5", headers={**ADV5, "X-Auth-Advertiser-Id": "0"}).status_code == 403


def test_auth_headers_hidden_from_openapi():
    spec = auth_app().app.openapi()
    params = spec["paths"]["/me"]["get"].get("parameters", [])
    assert not [p for p in params if p["in"] == "header"]


# ---- pgread ----


def test_postgres_settings_prefix(monkeypatch):
    monkeypatch.setenv("ANALYTICS_DB_URL", "postgresql://primary/app")
    monkeypatch.setenv("ANALYTICS_DB_READ_URL", "postgresql://replica/app")
    monkeypatch.setenv("ANALYTICS_DB_POOL_MAX", "3")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://other/app")
    s = postgres_settings("ANALYTICS_DB_")
    assert (s.url, s.read_url, s.pool_max) == ("postgresql://primary/app", "postgresql://replica/app", 3)
    assert postgres_settings().url == "postgresql://other/app"


class FakePool:
    def __init__(self, name, fail=None):
        self.name, self.fail, self.used = name, fail, 0

    @asynccontextmanager
    async def connection(self, timeout=None):  # noqa: ASYNC109 — mirrors AsyncConnectionPool
        self.used += 1
        if self.fail:
            raise self.fail
        yield self.name


class FakeDb:
    def __init__(self, replica_fail=None, with_replica=True):
        self.primary = FakePool("primary")
        self.replica = FakePool("replica", replica_fail) if with_replica else self.primary


async def which(conn):
    return conn


async def test_reads_prefer_replica():
    router = ReadRouter(FakeDb())
    assert await router.run(which) == "replica"


async def test_no_replica_uses_primary():
    router = ReadRouter(FakeDb(with_replica=False))
    assert not router.has_replica
    assert await router.run(which) == "primary"


@pytest.mark.parametrize("error", [PoolTimeout("timeout"), psycopg.OperationalError("refused")])
async def test_replica_failure_falls_back_and_cools_down(error):
    t = [100.0]
    db = FakeDb(replica_fail=error)
    router = ReadRouter(db, cooldown=5, clock=lambda: t[0])
    assert await router.run(which) == "primary"
    assert db.replica.used == 1
    assert await router.run(which) == "primary"
    assert db.replica.used == 1  # skipped during the cooldown
    t[0] += 6
    db.replica.fail = None
    assert await router.run(which) == "replica"


async def test_statement_timeout_is_not_retried_on_primary():
    db = FakeDb(replica_fail=psycopg.errors.QueryCanceled("canceling statement due to statement timeout"))
    router = ReadRouter(db)
    with pytest.raises(psycopg.errors.QueryCanceled):
        await router.run(which)
    assert db.primary.used == 0


def test_db_error_handler_503():
    app = create_app(ServiceSettings(service_name="t"))
    install_db_error_handlers(app)

    @app.get("/x")
    async def x():
        raise PoolTimeout("no connection")

    res = TestClient(app).get("/x")
    assert res.status_code == 503
    assert res.json() == {"detail": "database unavailable"}


# ---- migrate ----


def test_migrate_url_env_argument(monkeypatch):
    from sdl_common import migrate

    seen = {}
    monkeypatch.setattr(migrate, "migrate", lambda pkg, url: seen.update(pkg=pkg, url=url) or [])
    monkeypatch.setenv("ANALYTICS_DB_URL", "postgresql://a/app")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://p/app")
    monkeypatch.setattr("sys.argv", ["migrate", "x.migrations", "ANALYTICS_DB_URL"])
    assert migrate.main() == 0 and seen == {"pkg": "x.migrations", "url": "postgresql://a/app"}
    monkeypatch.setattr("sys.argv", ["migrate", "x.migrations"])
    assert migrate.main() == 0 and seen["url"] == "postgresql://p/app"
    monkeypatch.setattr("sys.argv", ["migrate", "x.migrations", "MISSING_URL"])
    assert migrate.main() == 2
