import asyncio
import logging
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from sdl_common.logging import configure_logging
from sdl_common.settings import ServiceSettings

log = logging.getLogger("sdl.app")

# A readiness check is an async callable that raises (or times out) when a dependency is unhealthy.
ReadinessCheck = Callable[[], Awaitable[object]]
Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]

_PROBES = frozenset(("/healthz", "/readyz", "/metrics"))


class _ServedByMiddleware:
    """`X-Served-By` header + one JSON access-log line per request (probes excluded).

    A plain ASGI middleware rather than `@app.middleware("http")`: Starlette's BaseHTTPMiddleware
    wraps every request in an extra task group and memory streams, which measured as ~half of the
    CPU of a small endpoint. Behaviour is unchanged: same header, same log line and fields, and no
    log line when the app raises (the error is logged by the server).

    `access_log_sample` < 1 logs only that fraction of 2xx/3xx requests (hot paths at thousands of
    requests/s); 4xx/5xx are always logged.
    """

    def __init__(self, app: ASGIApp, served_by: str, access_log_sample: float = 1.0) -> None:
        self.app = app
        self.served_by = served_by
        self.sample = access_log_sample

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        start = time.perf_counter()
        status = 0

        async def send_with_header(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                MutableHeaders(scope=message)["X-Served-By"] = self.served_by
            await send(message)

        await self.app(scope, receive, send_with_header)
        path = scope["path"]
        if path in _PROBES:
            return
        if status < 400 and self.sample < 1.0 and random.random() >= self.sample:
            return
        log.info(
            "request",
            extra={
                "extra_fields": {
                    "method": scope["method"],
                    "path": path,
                    "status": status,
                    "ms": round((time.perf_counter() - start) * 1000, 1),
                }
            },
        )


def create_app(
    settings: ServiceSettings,
    *,
    title: str | None = None,
    lifespan: Lifespan | None = None,
    readiness: Sequence[tuple[str, ReadinessCheck]] | Callable[[], Sequence[tuple[str, ReadinessCheck]]] = (),
    readiness_timeout: float = 2.0,
    access_log_sample: float = 1.0,
) -> FastAPI:
    """Build a FastAPI app with the lab's standard plumbing.

    - GET /healthz  liveness: the process is up (never checks dependencies — a DB outage
                    must not restart pods)
    - GET /readyz   readiness: runs every check; 503 takes the pod out of the gateway's rotation
    - GET /metrics  Prometheus metrics (scraped via the chart's ServiceMonitor)
    - every response carries `X-Served-By: <pod>@<node>` so load balancing and failover are visible
    - one JSON access-log line per request; `access_log_sample` (0..1, default 1 = all) samples the
      2xx/3xx lines of high-rate services, errors are always logged
    """
    configure_logging(settings.service_name, settings.pod_name, settings.log_level)

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info("starting", extra={"extra_fields": {"node": settings.node_name}})
        if lifespan is None:
            yield
        else:
            async with lifespan(app):
                yield
        log.info("stopped")

    app = FastAPI(title=title or settings.service_name, root_path=settings.root_path, lifespan=_lifespan)
    served_by = f"{settings.pod_name}@{settings.node_name}"

    app.add_middleware(_ServedByMiddleware, served_by=served_by, access_log_sample=access_log_sample)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
        checks = readiness() if callable(readiness) else readiness
        results: dict[str, str] = {}
        ok = True
        for name, check in checks:
            try:
                await asyncio.wait_for(check(), timeout=readiness_timeout)
                results[name] = "ok"
            except Exception as exc:  # noqa: BLE001 — any failure means not ready
                ok = False
                results[name] = f"fail: {type(exc).__name__}: {exc}"[:200]
        return JSONResponse(
            {"status": "ok" if ok else "fail", "checks": results}, status_code=200 if ok else 503
        )

    Instrumentator(excluded_handlers=["/healthz", "/readyz", "/metrics"]).instrument(app).expose(
        app, include_in_schema=False
    )
    return app


def run(app_path: str, settings: ServiceSettings) -> None:
    """Entry point for `python -m <module>`: uvicorn with graceful shutdown."""
    uvicorn.run(
        app_path,
        host="0.0.0.0",
        port=settings.port,
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_graceful_shutdown=20,
        log_config=None,
    )
