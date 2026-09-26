"""Shared plumbing for lab services.

- `create_app` / `run`      : FastAPI factory with /healthz, /readyz, /metrics, JSON logs, X-Served-By
- `ServiceSettings`         : env-driven base settings (SERVICE_NAME, ROOT_PATH, POD_NAME, NODE_NAME, ...)
- `sdl_common.postgres`     : connection-contract settings (POSTGRES_*) + async pool   [extra: postgres]
- `sdl_common.pgread`       : any-prefix Postgres settings, replica-first reads, 503 on DB down [extra: postgres]
- `sdl_common.migrate`      : advisory-locked SQL migration runner                      [extra: postgres]
- `sdl_common.auth`         : identity from gateway JWT claim headers (401/403 dependencies)
- `sdl_common.contract`     : assert a FastAPI app implements its design/contracts/openapi spec
"""

from sdl_common.app import ReadinessCheck, create_app, run
from sdl_common.settings import ServiceSettings

__all__ = ["ReadinessCheck", "ServiceSettings", "create_app", "run"]
