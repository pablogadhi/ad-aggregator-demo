# click-aggregator (Flink SQL)

Consumes Kafka `clicks` (12 partitions, JSON per `design/contracts/events/clicks.schema.json`) and
keeps `analytics-db.click_counts` (`design/contracts/db/analytics-db.sql`) up to date with
per-(ad, UTC minute) click counts, within ~1–2 s of a click being accepted (spec §6.1).

```
pipelines/click-aggregator/
├── Tiltfile                     # docker_build + FlinkDeployment (+ a "job RUNNING" check)
├── Dockerfile                   # maven build (runs the tests) -> flink:2.2.1-scala_2.12-java17 + connectors
├── docker/sdl-entrypoint.sh     # adds s3.* settings from aws-conn env, then the image's entrypoint
├── deploy/flinkdeployment.yaml  # checkpoints, HA, restart strategy, autoscaler, zone spread
└── job/                         # click-aggregator.sql + a ~150-line Java runner + KEEP_CLICK UDF + tests
```

## Versions (pinned together)

| What | Version |
| --- | --- |
| Flink | 2.2.1 (`flink:2.2.1-scala_2.12-java17`, `flinkVersion: v2_2`) — operator 1.16.1 |
| Kafka SQL connector | `flink-sql-connector-kafka:5.0.0-2.2` |
| JDBC connector | `flink-connector-jdbc-core` + `flink-connector-jdbc-postgres` `4.1.0-2.2` (+ `flink-connector-base:2.2.1`) |
| Postgres driver | `org.postgresql:postgresql:42.7.13` |
| S3 | built-in `flink-s3-fs-presto-2.2.1.jar` (`ENABLE_BUILT_IN_PLUGINS`) |

Flink 2.2 is the newest line with both the Kafka and the JDBC connector released (same as
`infra/components/flink/README.md`).

The connectors and the driver are shaded into the job jar (`/opt/flink/usrlib`), not copied into
`/opt/flink/lib`: JDBC 4.x pulls in openlineage + jackson/commons-lang3/snakeyaml/httpclient5, which
would sit next to flink-dist's own copies on the system classpath. In the user jar they load
child-first. The shade step merges `META-INF/services`, so the Kafka and JDBC table factories are
both discoverable.

## Why Flink SQL (with a tiny Java runner) and not PyFlink

The job is a textbook continuous group aggregation, so SQL is the most direct statement of it, and
the planner gives us for free what the design needs: mini-batching, local-global (two-phase)
aggregation, state TTL and the JDBC upsert sink. PyFlink would add a Python worker per TaskManager
and (for the one UDF) cross-process serialization on the hot path, and a much bigger image. The
Java runner only substitutes `${VAR}`s from the environment, registers the `KEEP_CLICK` UDF (needed
for a real "late clicks dropped" metric) and executes `job/src/main/resources/click-aggregator.sql`.

## What it computes

```
clicks ─► KEEP_CLICK filter ─► stage 1: GROUP BY ad_id, advertiser_id, salt, minute → COUNT(*)
                               ─► stage 2: GROUP BY ad_id, minute → SUM(partial), MAX(advertiser_id)
                               ─► JDBC upsert click_counts (PK ad_id, minute)
```

- **Continuous, not windowed:** every mini-batch (~1 s) emits updated counts, so a lone click in a
  quiet minute shows up within seconds; nothing waits for a watermark (idle partitions can't stall it).
- **No hot key:** a salted hot ad is up to 12 stage-1 keys; stage 2 receives at most one update per
  stage-1 key per second, and TWO_PHASE pre-aggregates stage 2 per subtask before the shuffle.
- **Stage 2 key = sink PK.** Stage 2 groups by `(ad_id, minute)` and carries `advertiser_id` as
  `MAX()` (constant per ad) instead of grouping by it. Grouping by it would make the upsert key
  `(ad_id, advertiser_id, minute)` ≠ PK, and the planner would then send UPDATE_BEFORE rows to the
  JDBC sink (executed as DELETEs: rows would flicker) and add a SinkUpsertMaterializer. A test
  asserts the plan: 2× Local/GlobalGroupAggregate, no materializer, sink input `[I,UA,D]`.
- `minute` = `FLOOR(clicked_at TO MINUTE)` in UTC; `updated_at` = when the count last changed.

## Delivery guarantees ("no clicks lost")

- **Source:** offsets are part of the checkpoint (10 s, EXACTLY_ONCE, incremental RocksDB, in
  `s3://flink-state/click-aggregator/checkpoints`); they are also committed to group
  `click-aggregator` for lag visibility. First start (no checkpoint, no group offsets): earliest.
- **Sink:** idempotent upsert of the *absolute* count (`INSERT … ON CONFLICT (ad_id, minute) DO UPDATE`).
  After any failure the job restores state + offsets from the last checkpoint and replays; replayed
  rows overwrite with the same values → **effectively exactly-once counts**. The sink also flushes
  on every checkpoint, so a checkpoint only completes once its rows are in Postgres.
- **Failures:** the sink retries a failing flush 10× (linear backoff), then the job restarts with
  exponential delay (1 s → 30 s) from the last checkpoint — this is also how it "waits" for the
  `click_counts` table until `analytics` has migrated it, and how it rides out an analytics-db outage.
- **JobManager loss:** Kubernetes HA (`high-availability.storageDir: s3://flink-state/click-aggregator/ha`).
- **Late events:** clicks older than `LATE_CUTOFF` (1 h, processing time) are dropped by
  `KEEP_CLICK` — metric `clicksDroppedLate` (+ a rate-limited WARN log); malformed records are
  dropped as `clicksDroppedMalformed` (`json.ignore-parse-errors`, so a poison record can't
  crash-loop the job). State TTL is 2 h, so no key can expire while it may still be updated
  (which would overwrite a row with a smaller count). **Documented limit:** an outage longer than
  1 h loses those clicks' counts; fixing that needs a batch reconciliation path.

## Scaling

Operator job autoscaler, per-vertex parallelism **3–12** (12 = partitions; 3 = one per zone),
target utilization 0.7, lab-tuned windows (3 min metrics window, 1 min stabilization, 5 min
scale-down delay). `pipeline.max-parallelism: 120` keeps key groups stable across rescales;
`jobmanager.scheduler: adaptive` lets the operator rescale in place (otherwise it redeploys from
the latest checkpoint, `upgradeMode: last-state`). 1 slot per TM ⇒ TMs = parallelism; TMs are
spread over zones. Next bottleneck: the single analytics-db primary (JDBC sink batches ≤ 1,000
rows / 1 s per subtask).

## Configuration

Env from `kafka-conn` (`KAFKA_BOOTSTRAP_SERVERS`), `analytics-db-conn` (`ANALYTICS_DB_JDBC_URL`,
`ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD`), `aws-conn` (`AWS_ENDPOINT_URL`, `AWS_REGION`, keys) and
`CLICKS_TOPIC`, `CONSUMER_GROUP`, `STATE_BUCKET`, `CHECKPOINT_INTERVAL`, `STATE_TTL`, `LATE_CUTOFF`
(design/contracts/config.md). Flink's config has no env substitution and the operator mounts it
read-only, so `docker/sdl-entrypoint.sh` (`kubernetes.entry.path`) copies it and appends the `s3.*`
keys from the `AWS_*` env — credentials stay in Secrets. Switch the S3 plugin to
`flink-s3-fs-hadoop-2.2.1.jar` in `ENABLE_BUILT_IN_PLUGINS` if needed; the same `s3.*` keys apply.

## Tests (no cluster)

`docker build pipelines/click-aggregator` runs `mvn verify` (8 tests):
- the SQL renders (all `${VAR}`s, quotes escaped) and fails fast without connection env;
- the real job (Kafka + JDBC DDL) plans as described above;
- end to end on a local MiniCluster with the Kafka source swapped for a JSON file in the same
  format: salted clicks of one ad over 5 salts + 2 minutes count exactly, UTC minute buckets,
  RFC 3339 `…Z` timestamps parse, late and malformed records are dropped;
- `KEEP_CLICK` unit tests.

`FLINK_SKIP_TESTS=true make dev` skips them in Tilt rebuilds.

## Observe

- `kubectl -n apps get flinkdeployment click-aggregator` (job state, lifecycle), the Flink UI via
  `kubectl -n apps port-forward svc/click-aggregator-rest 8081`, or `$FLINK_REST_URL`.
- Grafana/Prometheus (PodMonitor `apps/flink-jobs`, reporter port 9249 from the operator defaults):
  `flink_taskmanager_job_task_operator_clicksDroppedLate`, Kafka source `pendingRecords` (lag),
  busy time, checkpoint durations; parallelism in `pipeline.jobvertex-parallelism-overrides`.
