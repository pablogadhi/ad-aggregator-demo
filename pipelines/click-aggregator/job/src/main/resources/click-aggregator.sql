-- click-aggregator (spec §6, §6.1): Kafka `clicks` -> per-(ad, minute) counts -> analytics-db.
-- ${VAR}s are filled from the environment by ClickAggregatorJob (fails fast on a missing one).
-- Statements end with ';' at the end of a line; `--` lines are comments.

-- Minute buckets are UTC (FLOOR(... TO MINUTE) and the TIMESTAMP casts use the session zone).
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'click-aggregator';
SET 'execution.checkpointing.interval' = '${CHECKPOINT_INTERVAL}';

-- Keys for a minute stop changing ~1 h after it (older clicks are dropped below), so a 2 h TTL
-- never expires a key that could still be updated -- which would overwrite the row with a
-- smaller count.
SET 'table.exec.state.ttl' = '${STATE_TTL}';

-- ~1 s mini-batches: each aggregate emits at most one update per key per second, and the sink
-- sees at most ~12 updates/s for a hot ad instead of one per click.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1 s';
SET 'table.exec.mini-batch.size' = '5000';
-- local-global aggregation: each subtask pre-aggregates before the shuffle (no hot key downstream).
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

-- Source: JSON per design/contracts/events/clicks.schema.json. Offsets live in checkpoints;
-- committing them to the group is only for visibility (lag dashboards). First start: earliest.
CREATE TABLE clicks (
  click_id      STRING,
  ad_id         BIGINT,
  advertiser_id BIGINT,
  salt          INT,
  user_id       STRING,
  clicked_at    TIMESTAMP_LTZ(3),
  receiver      STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${CLICKS_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = '${CONSUMER_GROUP}',
  'properties.auto.offset.reset' = 'earliest',
  'properties.commit.offsets.on.checkpoint' = 'true',
  'scan.startup.mode' = 'group-offsets',
  'scan.topic-partition-discovery.interval' = '1 min',
  'format' = 'json',
  -- "2026-09-26T12:00:01.123Z" (RFC 3339, trailing Z) -> TIMESTAMP_LTZ
  'json.timestamp-format.standard' = 'ISO-8601',
  -- a malformed record must not crash-loop the job: its fields become NULL and KEEP_CLICK drops
  -- it (metric clicksDroppedMalformed)
  'json.ignore-parse-errors' = 'true'
);

-- Sink: analytics-db click_counts (design/contracts/db/analytics-db.sql, migrated by `analytics`).
-- Upsert on the primary key -> INSERT ... ON CONFLICT (ad_id, minute) DO UPDATE with the absolute
-- count: replays after a restore rewrite the same values (idempotent => effectively exactly-once).
-- The table may not exist yet: failed flushes are retried, then the job restarts (exponential
-- delay) from the last checkpoint until it does.
CREATE TABLE click_counts (
  ad_id         BIGINT,
  advertiser_id BIGINT,
  `minute`      TIMESTAMP(3),
  click_count   BIGINT,
  updated_at    TIMESTAMP(3),
  PRIMARY KEY (ad_id, `minute`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '${ANALYTICS_DB_JDBC_URL}',
  'table-name' = 'click_counts',
  'driver' = 'org.postgresql.Driver',
  'username' = '${ANALYTICS_DB_USER}',
  'password' = '${ANALYTICS_DB_PASSWORD}',
  'sink.buffer-flush.interval' = '1 s',
  'sink.buffer-flush.max-rows' = '1000',
  'sink.max-retries' = '10'
);

-- Two-stage continuous aggregation (no windows: a lone click in a quiet minute is emitted within
-- ~1 s, and there is no watermark to stall on idle partitions).
--   stage 1: (ad_id, advertiser_id, salt, minute) -> COUNT(*)   a hot ad is up to 12 keys
--   stage 2: (ad_id, minute) -> SUM(partial)
-- Stage 2 groups by (ad_id, minute) -- exactly the sink's primary key -- and carries advertiser_id
-- with MAX() (it is constant per ad). Grouping by advertiser_id too would give the result the
-- upsert key (ad_id, advertiser_id, minute) != PK, which makes the planner send UPDATE_BEFORE rows
-- to the JDBC sink (executed as DELETEs: rows would briefly vanish) and add a SinkUpsertMaterializer.
-- updated_at is when the count was last changed (the analytics API's freshness signal).
INSERT INTO click_counts
SELECT
  ad_id,
  MAX(advertiser_id)                        AS advertiser_id,
  `minute`,
  SUM(partial)                              AS click_count,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))   AS updated_at
FROM (
  SELECT
    ad_id,
    advertiser_id,
    salt,
    CAST(FLOOR(clicked_at TO MINUTE) AS TIMESTAMP(3)) AS `minute`,
    COUNT(*)                                          AS partial
  FROM clicks
  -- drops clicks older than LATE_CUTOFF (processing time) and malformed records, with metrics
  WHERE KEEP_CLICK(ad_id, advertiser_id, salt, clicked_at, CAST(${LATE_CUTOFF_MS} AS BIGINT))
  GROUP BY ad_id, advertiser_id, salt, CAST(FLOOR(clicked_at TO MINUTE) AS TIMESTAMP(3))
)
GROUP BY ad_id, `minute`;
