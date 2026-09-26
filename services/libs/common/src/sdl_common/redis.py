"""Redis via the `redis-conn` connection contract (env: REDIS_URL, REDIS_MODE).   [extra: redis]

    from sdl_common.redis import RedisSettings, connect

    client = connect(RedisSettings(), socket_timeout=0.5)   # redis.asyncio.Redis or RedisCluster
    await client.set("k", "v")
    await client.aclose()

REDIS_MODE=cluster (component profile `ha`) returns `redis.asyncio.cluster.RedisCluster`, which
discovers the shards from the URL's node and routes every key to its slot; `standalone` (profile
`small`) returns a plain `redis.asyncio.Redis`. Keys that must be used together in one command,
pipeline-transaction or Lua script need a shared hash tag (`{...}`) to live in one cluster slot.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_", extra="ignore")

    url: str
    mode: Literal["standalone", "cluster"] = "standalone"


def connect(settings: RedisSettings, **kwargs: Any) -> Redis | RedisCluster:
    """Build a client for the configured mode (lazy: connects on first use). `decode_responses`
    defaults to True; any other kwargs (timeouts, retry, ...) are passed through."""
    kwargs.setdefault("decode_responses", True)
    if settings.mode == "cluster":
        return RedisCluster.from_url(settings.url, **kwargs)
    return Redis.from_url(settings.url, **kwargs)
