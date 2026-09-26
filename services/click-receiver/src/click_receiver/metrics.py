"""Prometheus metrics (default registry, exposed on /metrics by sdl_common.create_app)."""

from prometheus_client import Counter, Gauge, Histogram

CLICKS = Counter(
    "clicks_total",
    "Click requests on a known ad, by outcome (rejected = not durably recorded -> 503)",
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
