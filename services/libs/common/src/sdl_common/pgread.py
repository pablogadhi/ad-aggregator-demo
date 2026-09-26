"""Helpers on top of `sdl_common.postgres` for any connection-contract prefix.   [extra: postgres]

    from sdl_common.pgread import ReadRouter, install_db_error_handlers, postgres_settings

    db = Database(postgres_settings("ANALYTICS_DB_"))      # ANALYTICS_DB_URL, ANALYTICS_DB_READ_URL, ...
    reads = ReadRouter(db)
    rows = await reads.run(lambda conn: fetch_rows(conn, ...))   # replica, primary on failure
    install_db_error_handlers(app)                          # pool timeout / DB down -> 503 JSON

`postgres_settings(prefix)` reads the same keys as `PostgresSettings` (URL, READ_URL, POOL_MIN,
POOL_MAX) under another prefix: a component installed as instance `analytics-db` publishes
`analytics-db-conn`, which the chart turns into `ANALYTICS_DB_*` env vars.
"""

import logging
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout

from sdl_common.postgres import Database, PostgresSettings

log = logging.getLogger("sdl.pgread")
T = TypeVar("T")

# Errors that mean "this database/pool is not usable right now" (as opposed to a bad query).
UNAVAILABLE_ERRORS: tuple[type[Exception], ...] = (PoolTimeout, psycopg.OperationalError)


def postgres_settings(prefix: str = "POSTGRES_", **overrides) -> PostgresSettings:
    """PostgresSettings bound to another env prefix, e.g. "ANALYTICS_DB_" (trailing underscore included)."""
    return PostgresSettings(_env_prefix=prefix, **overrides)


class ReadRouter:
    """Run read-only work on the replica pool, falling back to the primary.

    Why: replicas take read load off the primary, but a replica outage (or the `-ro` service having
    no endpoints during a failover) must not fail reads. After a replica failure the router skips
    the replica for `cooldown` seconds (a tiny circuit breaker) so every request doesn't pay the
    `replica_timeout` while it is down. Only use it for idempotent reads — `fn` may run twice.
    """

    def __init__(
        self,
        db: Database,
        *,
        replica_timeout: float = 1.0,
        primary_timeout: float = 5.0,
        cooldown: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.db = db
        self.replica_timeout = replica_timeout
        self.primary_timeout = primary_timeout
        self.cooldown = cooldown
        self._clock = clock
        self._skip_replica_until = 0.0

    @property
    def has_replica(self) -> bool:
        return self.db.replica is not self.db.primary

    def replica_available(self) -> bool:
        return self.has_replica and self._clock() >= self._skip_replica_until

    async def run(self, fn: Callable[[psycopg.AsyncConnection], Awaitable[T]]) -> T:
        if self.replica_available():
            try:
                async with self.db.replica.connection(timeout=self.replica_timeout) as conn:
                    return await fn(conn)
            except UNAVAILABLE_ERRORS as exc:
                if isinstance(exc, psycopg.errors.QueryCanceled):
                    raise  # statement_timeout: the query is the problem, not the replica — don't rerun it
                self._skip_replica_until = self._clock() + self.cooldown
                log.warning(
                    "replica unavailable, reading from primary",
                    extra={"extra_fields": {"error": f"{type(exc).__name__}: {exc}"[:200]}},
                )
        async with self.db.primary.connection(timeout=self.primary_timeout) as conn:
            return await fn(conn)

    async def check(self) -> None:
        """Readiness: reads can be served (replica or primary answers)."""

        async def ping(conn: psycopg.AsyncConnection) -> None:
            await conn.execute("SELECT 1")

        await self.run(ping)


def install_db_error_handlers(app: FastAPI) -> None:
    """Map "database unavailable" errors to 503 {"detail": ...} instead of a 500 stack trace."""

    async def unavailable(request: Request, exc: Exception) -> JSONResponse:
        log.warning(
            "database unavailable",
            extra={"extra_fields": {"path": request.url.path, "error": f"{type(exc).__name__}: {exc}"[:200]}},
        )
        return JSONResponse({"detail": "database unavailable"}, status_code=503, headers={"Retry-After": "1"})

    for exc_type in UNAVAILABLE_ERRORS:
        app.add_exception_handler(exc_type, unavailable)
