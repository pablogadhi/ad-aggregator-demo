-- Analytics DB (component instance `analytics-db`, database `app`). Owner: analytics (runs the
-- migration). Written only by the click-aggregator Flink job (JDBC upsert keyed by the PK);
-- read by analytics through ANALYTICS_DB_READ_URL.
--
-- One row per (ad, minute bucket of the click's event time). The Flink job writes the *absolute*
-- count for the bucket (never an increment), so replays after a restore overwrite with the same
-- value: idempotent, effectively exactly-once.

CREATE TABLE IF NOT EXISTS click_counts (
    ad_id          bigint      NOT NULL,
    advertiser_id  bigint      NOT NULL,           -- denormalized from the click event
    minute         timestamptz NOT NULL,           -- start of the UTC minute (seconds = 0)
    click_count    bigint      NOT NULL CHECK (click_count >= 0),
    updated_at     timestamptz NOT NULL DEFAULT now(),  -- set by Flink: processing time of the last change
    PRIMARY KEY (ad_id, minute)
);

CREATE INDEX IF NOT EXISTS click_counts_advertiser_minute_idx
    ON click_counts (advertiser_id, minute) INCLUDE (ad_id, click_count);

-- Upsert the Flink sink performs (for reference):
--   INSERT INTO click_counts (ad_id, advertiser_id, minute, click_count, updated_at)
--   VALUES ($1, $2, $3, $4, $5)
--   ON CONFLICT (ad_id, minute) DO UPDATE
--     SET advertiser_id = EXCLUDED.advertiser_id, click_count = EXCLUDED.click_count,
--         updated_at = EXCLUDED.updated_at;
