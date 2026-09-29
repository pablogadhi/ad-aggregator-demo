"""Prometheus metrics (default registry, exposed on /metrics by sdl_common.create_app)."""

from prometheus_client import Counter, Gauge, Histogram

CLICKS = Counter(
    "clicks_total",
    "Click requests on a known ad, by outcome: accepted, duplicate; 503s: shed (deadline passed "
    "before producing, definite), rejected (produce failed before/without being sent, definite), "
    "ambiguous (timed out in flight: may have been persisted)",
    ["status", "hot"],
)
UNKNOWN_AD = Counter("click_unknown_ad_total", "Clicks on unknown or inactive ads (404)")
AD_LOOKUP_ERRORS = Counter("click_ad_lookup_errors_total", "Ad lookups that failed on both DB pools (503)")
DEDUP_FAILOPEN = Counter(
    "click_dedup_failopen_total", "Dedup checks that errored/timed out and were treated as new clicks"
)
PRODUCE_SECONDS = Histogram(
    "click_produce_seconds",
    "Time from produce() to the broker ack (acks=all)",
    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0),
)
HOT_ADS = Gauge("hot_ads", "Hot ads (temporary + permanent) in this receiver's view")
HOT_ADS_PERMANENT = Gauge("hot_ads_permanent", "Permanently hot ads in this receiver's view")
HOT_MARKINGS = Counter("hot_ad_markings_total", "New hot markings made by this receiver's flusher")
HOT_FLUSH_ERRORS = Counter("hot_counter_flush_errors_total", "Failed hot-ad counter flushes / markings")
HOT_REFRESH_ERRORS = Counter("hot_refresh_errors_total", "Failed hot-ad set refreshes (last set kept)")
HOT_COUNTS_DROPPED = Counter(
    "hot_counter_dropped_total", "Per-ad click counts dropped because the unflushed backlog was full"
)
REDIS_SLOT_REFRESHES = Counter(
    "click_redis_slot_refreshes_total",
    "Redis Cluster slot-map refreshes of the per-click client (reason: failures|periodic)",
    ["reason", "result"],
)
WARMUP_SECONDS = Gauge("click_warmup_seconds", "Duration of the warm-up before readiness")
REDIS_BREAKER_TRIPS = Counter(
    "click_redis_breaker_trips_total",
    "Times a Redis node's dedup circuit breaker opened (consecutive failures on that node)",
)
REDIS_BREAKER_SKIPS = Counter(
    "click_redis_breaker_skips_total",
    "Dedup checks skipped (failed open without a Redis call) because the key's node breaker was open; "
    "also counted in click_dedup_failopen_total",
)
REDIS_BREAKER_OPEN = Gauge("click_redis_breaker_open_nodes", "Redis nodes whose dedup breaker is open")
HOT_LOOP_BACKOFF = Gauge(
    "hot_loop_backoff_seconds",
    "Current delay of a hot-ad background loop (its interval unless failing)",
    ["loop"],
)
