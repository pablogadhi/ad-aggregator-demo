from sdl_common import ServiceSettings


class Settings(ServiceSettings):
    """Service knobs (design/contracts/config.md, click-receiver row). Connection env vars
    (POSTGRES_*, KAFKA_*, REDIS_*) are read by the sdl_common settings classes in the lifespan."""

    service_name: str = "click-receiver"

    clicks_topic: str = "clicks"
    dedup_ttl_seconds: int = 600
    ad_cache_ttl_seconds: float = 60
    ad_cache_max_entries: int = 50_000
    db_timeout_ms: int = 800  # per ad lookup query (cache miss only)
    redis_timeout_ms: int = 50  # dedup SET NX budget; beyond it we fail open
    kafka_delivery_timeout_ms: int = 1500  # librdkafka delivery.timeout.ms (gateway timeout is 2 s)

    hot_threshold_clicks_10m: int = 1200
    hot_mark_ttl_seconds: int = 600
    hot_permanent_after_marks: int = 10
    hot_salt_buckets: int = 12
    hot_salting_enabled: bool = True
    hot_flush_interval_ms: int = 1000
    hot_refresh_interval_ms: int = 2000
    hot_pending_max_ads: int = 10_000  # bound on un-flushed counters while Redis is down

    @property
    def receiver_id(self) -> str:
        return f"{self.pod_name}@{self.node_name}"
