# Ad Click Aggregator — spec

> Written by the architect step of `/build-design` from `diagram.excalidraw` + `diagram.png`
> (parsed into `diagram.graph.md`). Every builder agent treats this file + `contracts/` as the
> source of truth. Decisions confirmed with the user are marked **(decided)**.

## 1. Problem

Ads are shown on a website/app. When a user clicks an ad, the system records the click and
redirects the user to the advertiser's site. Advertisers query click metrics per ad and per
advertiser over time with 1-minute granularity. Clicks go through a stateless receiver pool into
Kafka, and a Flink job aggregates them into per-minute counts in a separate analytics database.

## 2. Requirements

**Functional**

1. Users can click an ad and are redirected to the advertiser's `redirect_url`.
2. Advertisers can query click metrics over time with a minimum granularity of 1 minute: per ad
   and per advertiser, rolled up to `minute | hour | day`.
3. Advertisers manage their ads (CRUD); the site reads active ads for display.
4. A click is unique per `(user_id, ad_id)`: repeated clicks by the same user on the same ad
   within 10 minutes still redirect but are **not counted** (diagram: Redis TTL 10 min +
   "unique together on user_id and ad_id") **(decided)**.
5. **Hot ads are salted (decided).** Receivers keep a per-ad count of recent clicks (last 10 min)
   in Redis; an ad above the threshold is marked **hot** for 10 min and its clicks are spread over
   several Kafka partitions by salting the key. Every hot-marking increments a per-ad counter; after
   **10 markings the ad is permanently hot** (no flip-flopping between salted/unsalted).
6. **Every stage scales horizontally (decided):** receivers (HPA), Kafka partitions (12), the Flink
   job (operator autoscaler, parallelism 3–12) and a two-stage aggregation that has no hot keys.

**Non-functional (as stated)**

- High availability.
- **No clicks are lost on failure**: an accepted click is always counted exactly once in the
  aggregates, even across broker, Flink and DB failures.
- Click capture + redirect latency < 100 ms.
- Metrics query latency < 500 ms.
- Eventual consistency of 1 min or less: an accepted click appears in the analytics within 60 s.
- Scale: 10M active ads, ~100M clicks/day.

## 3. Laptop scale-down

| Diagram says                                | Lab target                                                             | Why it still means something                                                                                            |
| ------------------------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| ~100M clicks/day (~1.2k rps avg, ~10k peak) | 500 rps sustained for 5 min, 1,000 rps burst for 1 min                 | same path: gateway → N receivers → keyed Kafka partitions → parallel Flink → upserts. k6 measures p95 and loss.        |
| 10M active ads                              | 2,000 ads across 100 advertisers, created by the load test's setup     | enough keys to spread over all partitions; in-process ad cache hit rate stays realistic (hot set ≪ total)               |
| Click Receiver pool                         | `click-receiver` HPA 3–9 replicas (CPU), spread over zones             | zone loss removes a third of capacity; scale-out is observable under the burst                                         |
| Kafka cluster (3 brokers, N partitions)     | 3 brokers (one per zone), topic `clicks` 12 partitions, RF 3, minISR 2 | a broker can die with no loss (`acks=all`); 12 partitions = headroom for Flink to scale to 12 source subtasks           |
| Flink cluster, N aggregators                | 1 JobManager + 3–12 TaskManagers × 1 slot (operator autoscaler)        | same scale-out mechanism as prod (lag/busy-time driven rescale from a checkpoint); TM/JM kill exercises recovery       |
| hot ads (viral ad)                          | one ad receiving 20 % of the load (100 rps); threshold 1,200 clicks/10 min | same skew; salting spreads it over partitions, measurable per partition                                             |
| Redis cluster                               | Redis Cluster 3 primaries + 3 replicas across zones                    | same client mode (cluster), primary failover behaviour                                                                  |
| 2 PostgreSQL DBs                            | 2 CNPG clusters × 3 instances (`postgres`, `analytics-db`) **(decided)** | independent failure domains: analytics DB down ≠ clicks down                                                          |
| < 100 ms click, < 500 ms query              | same thresholds (p95), at the lab rates above                          | latency targets don't scale; they're per request                                                                        |
| ≤ 1 min eventual consistency                | click visible in analytics ≤ 60 s (p95 of reconciliation lag ≤ 15 s)   | same mechanism (continuous aggregation + upsert)                                                                        |

## 4. Components (infra)

| Component (instance)      | Profile | Status | Connection contract (secret → env prefix)                                                                                               | Design-specific setup (`infra/design/`)                                   |
| ------------------------- | ------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| postgres (`postgres`)     | ha      | exists | `postgres-conn` → `POSTGRES_URL`, `POSTGRES_READ_URL`, `POSTGRES_HOST`, … + **new key `JDBC_URL`**                                      | none (ads schema is migrated by `ad-placement`)                           |
| postgres (`analytics-db`) | ha      | extend | `analytics-db-conn` → `ANALYTICS_DB_URL`, `ANALYTICS_DB_READ_URL`, `ANALYTICS_DB_JDBC_URL`, `ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD`, … | none (schema migrated by `analytics`)                                     |
| kafka                     | ha      | new    | `kafka-conn` → `KAFKA_BOOTSTRAP_SERVERS`                                                                                                | `KafkaTopic clicks`: 12 partitions, RF 3, `min.insync.replicas=2`, 24 h   |
| redis                     | ha      | new    | `redis-conn` → `REDIS_URL`, `REDIS_MODE` (`cluster` in ha, `standalone` in small), `REDIS_HOST`, `REDIS_PORT`                           | —                                                                         |
| aws (Floci S3)            | small   | new    | `aws-conn` → `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` **(decided)**                                | bucket `flink-state` (Job)                                                |
| flink (operator)          | small   | new    | `flink-conn` → `FLINK_REST_URL` (unused by services)                                                                                    | operator watches namespace `apps`; job autoscaler available (built into the operator) |
| — (design)                | —       | —      | `jwt-conn` → `JWT_PRIVATE_KEY_PEM`, `JWT_PUBLIC_KEY_PEM`, `JWT_KID`, `JWT_ISSUER`, `JWT_AUDIENCE`                                                             | RSA keypair generated at `make up`; Envoy `SecurityPolicy` (see §4.2)     |

### 4.1 Multiple instances of one component **(decided)**

`stack.yaml` entries take an optional `instance:` (default = component name). `make up` calls
`infra/components/<name>/install.sh <profile> <instance>` and `make smoke` calls
`smoke.sh <instance>` (already implemented in `scripts/stack.py`, `up.sh`, `smoke-all.sh`).
The postgres component must honour it: CNPG `Cluster` named `<instance>` (services
`<instance>-rw/-ro`), PodMonitor `<instance>`, secret `apps/<instance>-conn`, and `uninstall.sh
<instance>`. Instance `postgres` must stay byte-for-byte compatible with today's behaviour. Both
instances use database `app`. Add the key `JDBC_URL` (`jdbc:postgresql://<host>:5432/app`, no
credentials) to the postgres contract (for Flink). Document `instance` in `AUTHORING.md`.

### 4.2 Gateway: auth, logs **(decided: JWT at the gateway)**

- Envoy Gateway `SecurityPolicy` with a JWT provider on the HTTPRoutes **`ad-placement`** and
  **`analytics`** (the chart names the route after the release). Tokens are RS256, `iss` =
  `JWT_ISSUER` (`ad-aggregator-auth`), `aud` = `JWT_AUDIENCE` (`ad-aggregator`). JWKS: the public key
  of the keypair in `jwt-conn` (inline/local JWKS, or remote JWKS from `auth`'s
  `/.well-known/jwks.json`, whichever the pinned Envoy Gateway version supports reliably).
- `claimToHeaders`: `sub` → `X-Auth-Sub`, `role` → `X-Auth-Role`, `advertiser_id` →
  `X-Auth-Advertiser-Id`. Every token carries all three claims (strings), so the gateway always
  overwrites client-supplied copies of these headers; the smoke test must prove a spoofed
  `X-Auth-Advertiser-Id` is overwritten.
- No JWT on `auth` and `click-receiver` routes (end users click plain links).
- Missing/invalid token → 401 from the gateway. Services do authorization (role / owner → 403).
- `BackendTrafficPolicy` on `click-receiver`: request timeout 2 s, no retries (the receiver is not
  idempotent across pods: a retried click after a Kafka ack would just be a dedup hit, but a
  retry must never be invisible to the client).
- Access logs: Envoy's default access log is enough ("logs" in the diagram).

## 5. Services

| Service          | Kind                       | Responsibilities                                                                                                   | API (contract)                                            | Consumes | Produces         | Stores                                   |
| ---------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------- | -------- | ---------------- | ---------------------------------------- |
| auth             | FastAPI, public (no JWT)   | demo login: mints RS256 JWTs for `viewer` / `advertiser`; publishes JWKS                                          | `contracts/openapi/auth.yaml` at `/api/auth`              | —        | —                | — (key from `jwt-conn`)                  |
| ad-placement     | FastAPI, public (JWT)      | advertiser + ad CRUD (owner-scoped); list/read active ads for display                                             | `contracts/openapi/ad-placement.yaml` at `/api/ad-placement` | —      | —                | `postgres` (owns `contracts/db/postgres.sql`) |
| click-receiver   | FastAPI, public (no JWT)   | look up ad → dedup in Redis → salt key if hot → produce to Kafka (`acks=all`) → 302 **after** the ack; hot-ad detection | `contracts/openapi/click-receiver.yaml` at `/api/click-receiver` | — | topic `clicks`  | reads `postgres` (ads); Redis (dedup, hot-ad state) |
| analytics        | FastAPI, public (JWT)      | time-series queries over `click_counts` per ad / per advertiser with minute/hour/day rollups                       | `contracts/openapi/analytics.yaml` at `/api/analytics`    | —        | —                | `analytics-db` (owns `contracts/db/analytics-db.sql`) |
| click-aggregator | pipeline (Flink)           | consume `clicks`, two-stage count per `(ad_id, salt, minute)` → `(ad_id, minute)`, upsert into `analytics-db.click_counts`; autoscaled | —                                                         | `clicks` | —                | `analytics-db`; state in S3             |

Replicas: click-receiver HPA min 3 / max 9 (`autoscaling.targetCPU` ~500m), others 2 (chart spreads
across zones, PDB).

### 5.1 click-receiver — the hot path

1. `GET /click/{ad_id}?user_id=…` (browser link, 302) and `POST /clicks` (JSON, for SPAs and
   tests) share one code path.
2. Ad lookup: in-process cache (TTL 60 s, max ~50k entries) → `POSTGRES_READ_URL` → on a miss,
   retry on the primary (`POSTGRES_URL`) so a just-created ad isn't a 404 because of replica lag.
   Unknown or inactive ad → 404, nothing produced.
3. Dedup: `SET click:dedup:{ad_id}:{user_id} <click_id> NX EX 600` with a 50 ms timeout.
   - key existed → `duplicate`: redirect, **don't** produce.
   - Redis error/timeout → **fail open**: treat as new (availability + no loss beats exact dedup);
     count it in metric `click_dedup_failopen_total`.
4. Produce `ClickEvent` (§6) to `clicks`, key = `ad_id`, or `ad_id#salt` for a hot ad (§5.1.1),
   producer `acks=all`,
   `enable.idempotence=true`, delivery timeout ≤ 1.5 s; **await the delivery report**.
   - ack → `accepted`: 302 / 200.
   - failure/timeout → best-effort `DEL` of the dedup key, then **503** (no redirect). The click
     wasn't recorded, and the client may retry. Never answer `accepted` without an ack.
5. Response headers on every click response: `X-Click-Status: accepted | duplicate`,
   `X-Click-Id: <uuid>` (for duplicates: the new request's id), `X-Click-Hot: true | false` (the
   receiver's view when it handled the click). Metrics: counters
   `clicks_total{status="accepted|duplicate|rejected", hot="true|false"}`.
6. Readiness: Kafka metadata reachable + Postgres reachable. Redis is **not** in readiness
   (it fails open).

### 5.1.1 click-receiver — hot-ad detection and salting **(decided)**

Goal: a viral ad must not pin one Kafka partition (one broker leader, one Flink source subtask).
Constraint: the per-click path keeps **one** Redis round trip (the dedup `SET NX`); everything
below is batched or cached so the < 100 ms p95 target holds.

All per-ad keys use the hash tag `{a:<ad_id>}` so they live in one Redis Cluster slot (multi-key
commands and Lua work in cluster mode). Global keys live in their own slots.

| Key                           | Type   | Written by                                   | Meaning                                                                 |
| ----------------------------- | ------ | -------------------------------------------- | ----------------------------------------------------------------------- |
| `hot:{a:ID}:c:<epoch_minute>` | string | flusher: `INCRBY n` + `EXPIRE 660`           | accepted clicks for the ad in that UTC minute (all receivers combined)  |
| `hot:{a:ID}:flag`             | string | Lua mark script: `SET NX EX 600` / `EXPIRE`  | ad is currently hot (temporary)                                         |
| `hot:{a:ID}:marks`            | string | Lua mark script: `INCR` on each new marking  | how many times the ad was marked hot (never expires)                   |
| `hot:ads`                     | zset   | flusher: `ZADD ad_id <hot-until epoch s>`   | index of temporarily hot ads, for receivers to read in bulk             |
| `hot:perm`                    | set    | flusher: `SADD ad_id` when marks ≥ 10        | permanently hot ads                                                     |

1. **Count (batched):** each receiver keeps an in-memory `ad_id → accepted clicks` map and flushes
   it every `HOT_FLUSH_INTERVAL_MS` (1 s) with a pipeline of `INCRBY`+`EXPIRE` on the current
   minute bucket. Only accepted clicks count (duplicates don't). On Redis errors the batch is kept
   and merged into the next flush (bounded; drop + metric beyond 10k ads).
2. **Detect (per flushed ad):** 10-minute sliding count = sum of the buckets for the current minute
   and the 9 before it (`MGET`, same slot). If it is ≥ `HOT_THRESHOLD_CLICKS_10M` (1,200), run the
   **mark** Lua script on the ad's keys:
   - if `flag` doesn't exist: `SET flag 1 EX 600`, `INCR marks` → **new marking**
   - else: `EXPIRE flag 600` (still hot, extend)
   - returns `(new_marking, marks)`.
   Then `ZADD hot:ads <now+600> ad_id`, and if `marks ≥ HOT_PERMANENT_AFTER_MARKS` (10),
   `SADD hot:perm ad_id`. An ad stops being hot when its flag expires, i.e. 10 min after it fell
   below the threshold: that TTL is the hysteresis. Permanently hot ads are never un-marked
   (manual `SREM` to reset; documented).
3. **Read (cached):** every `HOT_REFRESH_INTERVAL_MS` (2 s) each receiver loads
   `ZRANGEBYSCORE hot:ads <now> +inf` + `SMEMBERS hot:perm` (+ `GET hot:{a:ID}:marks` for those ads) into an in-process set (and
   `ZREMRANGEBYSCORE hot:ads -inf <now>` to trim). A receiver learns about a new hot ad within
   ~1 s (flush) + 2 s (refresh). If Redis is down, it keeps the last known set.
4. **Salt:** for a hot ad, Kafka key = `"<ad_id>#<salt>"` with `salt = random(0 … HOT_SALT_BUCKETS-1)`
   (`HOT_SALT_BUCKETS` = 12 = partitions), and the event carries `salt`. Non-hot: key = `"<ad_id>"`,
   `salt = 0`. Salts go through the default murmur2 partitioner, so a few may collide on a
   partition; measured in the hot-ad load scenario. `HOT_SALTING_ENABLED` (default `true`) turns
   salting off (detection still runs) for the A/B experiment.
5. **Observability:** `GET /hot-ads` returns the receiver's current view (hot, permanent, marks);
   gauges `hot_ads` / `hot_ads_permanent`, counter `hot_ad_markings_total`.

### 5.2 auth — demo login

`POST /token` `{role: "viewer", user_id}` or `{role: "advertiser", advertiser_id}` → signed JWT,
1 h TTL. No password (demo); it does not check that the advertiser exists. Claims: `sub`
(user_id or `advertiser:<id>`), `role`, `advertiser_id` (string, `""` for viewers), `iss`, `aud`,
`exp`, `iat`, header `kid`. `GET /.well-known/jwks.json` publishes the public key.

### 5.3 ad-placement — authorization rules

- Any valid token (`viewer` or `advertiser`): `GET /ads`, `GET /ads/{ad_id}`, `POST /advertisers`
  (sign-up).
- Owner only (`X-Auth-Role == advertiser` and `X-Auth-Advertiser-Id == {advertiser_id}`, else 403):
  everything under `/advertisers/{advertiser_id}`.
- `DELETE` = soft delete (`active = false`); the receiver's cache means clicks may still be
  accepted for up to 60 s afterwards (documented, not a bug).

### 5.4 analytics

- Owner only (same rule as §5.3) on `/advertisers/{advertiser_id}/…`.
- Reads from `ANALYTICS_DB_READ_URL` (replica lag counts toward the 60 s freshness budget); falls
  back to `ANALYTICS_DB_URL` if the replica pool is unavailable.
- Rolls up minute rows with `date_trunc(granularity, minute)`; returns zero-filled buckets over
  `[from, to)`; at most 1,440 buckets, else 400. Defaults: `to = now`, `from = to - 1h`,
  `granularity = minute`. `from` is truncated down and the exclusive `to` rounded up to bucket
  boundaries, so the current (still-filling) bucket is included (details in the contract).
- `freshness.last_updated_at` = `max(updated_at)` of the rows in range, so the client can show lag.

## 6. Data & events

- DB schemas: `contracts/db/postgres.sql` (ads DB; owner `ad-placement`) and
  `contracts/db/analytics-db.sql` (owner `analytics`, which runs the migration; the Flink job only
  writes rows). The Flink sink retries until the table exists.
- Events: `contracts/events/clicks.schema.json`.

| Topic    | Key                  | Partitions | Producer       | Consumers                                 | Delivery semantics                                                                                                             |
| -------- | -------------------- | ---------- | -------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `clicks` | `ad_id`, or `ad_id#salt` for hot ads (§5.1.1) | 12 (RF 3, minISR 2, retention 24 h) | click-receiver | click-aggregator (group `click-aggregator`) | producer: `acks=all` + idempotent, ack before redirect. Consumer: Flink exactly-once state via checkpoints (10 s, S3); sink is an idempotent upsert keyed by `(ad_id, minute)` → **effectively exactly-once counts** |

### 6.1 click-aggregator (Flink) semantics

- Source: Kafka `clicks`, `KAFKA_BOOTSTRAP_SERVERS`, starting offsets = committed group offsets,
  else earliest. Offsets are stored in checkpoints (committed to Kafka only for visibility).
- Aggregation: **continuous (non-windowed) group aggregation**, emitting updated counts
  continuously so freshness is seconds and a lone click in a quiet minute is still emitted (a
  tumbling event-time window would wait for the watermark, which stalls when partitions are idle).
  **Two stages, so a hot ad is not a hot key inside Flink either** (salting alone isn't enough:
  grouping by `ad_id` would funnel every salted record back into one subtask):
  1. `GROUP BY ad_id, advertiser_id, salt, minute = floor(clicked_at TO MINUTE)` → `count(*)`
     (a hot ad is spread over up to 12 keys)
  2. `GROUP BY ad_id, advertiser_id, minute` → `SUM(partial)` (at most 12 updates/s per hot ad at
     1 s mini-batches, instead of one per click)
  Mini-batch ~1 s (`table.exec.mini-batch.*`), plus local-global aggregation
  (`table.optimizer.agg-phase-strategy: TWO_PHASE`) so stage 2 is pre-aggregated per subtask.
- Sink: JDBC upsert into `click_counts` with primary key `(ad_id, minute)` (Postgres `ON CONFLICT
  … DO UPDATE SET click_count = EXCLUDED.click_count, updated_at = now()`), flush ≤ 1 s, retries
  with backoff. On restore, the counts are recomputed from checkpointed state + replayed offsets
  and overwrite the rows → no loss, no double count.
- State TTL 2 h; events with `clicked_at` older than 1 h (processing time) are dropped and counted
  in a metric/log (a late event after its key's state expired would overwrite the row with a
  smaller count). Documented limit: an outage longer than 1 h loses counts; fixing that needs a
  batch reconciliation path (next experiment).
- Checkpoints: every 10 s, exactly-once mode, `s3://flink-state/click-aggregator/checkpoints`;
  JobManager HA (Kubernetes HA services) with storage dir `s3://flink-state/click-aggregator/ha`;
  S3 via `aws-conn` (endpoint, path-style access). Restart strategy: exponential delay.
- Deployment: `FlinkDeployment` in namespace `apps`, 1 JM + TMs with 1 slot each, spread across
  zones; the job is Flink SQL or PyFlink (builder's choice, justified in the README).
- **Horizontal scaling (decided):** Flink Kubernetes Operator **job autoscaler**
  (`job.autoscaler.enabled: true`) with parallelism bounds **3–12**: 12 is the partition count
  (a source can't use more subtasks than partitions), and 3 keeps one subtask per zone. It scales
  per vertex on busy time and Kafka lag (target utilization ~0.7, stabilization/metrics window of
  a few minutes, lab-tuned so a 5-min load run can trigger it). Rescaling redeploys from the latest
  S3 checkpoint, so no state is lost. `pipeline.max-parallelism` is fixed (e.g. 120) so key
  groups stay stable across rescales. Adaptive scheduler in-place rescaling is preferred if the
  pinned Flink/operator versions support it. With 1 slot per TM, TMs = parallelism. The JDBC sink
  shares the scaling; the single analytics-db primary is the next bottleneck (documented, measured
  in §10).

## 7. Configuration matrix

See `contracts/config.md` (authoritative).

## 8. Acceptance flows (→ `tests/e2e/`)

All through `http://localhost:8080`. Each test uses fresh advertisers/ads/user ids (uuid-based),
so tests can run repeatedly against the same cluster.

1. **Auth at the edge**: `GET /api/analytics/advertisers/1/clicks` without a token → 401; with an
   invalid signature → 401; with advertiser A's token on advertiser B's path → 403; a spoofed
   `X-Auth-Advertiser-Id` header on A's token for B's path → 403.
2. **Advertiser onboarding**: viewer token → `POST /api/ad-placement/advertisers` → 201;
   advertiser token → create 2 ads → `GET /advertisers/{id}/ads` lists both → `GET /ads` (viewer)
   includes them with `click_url`.
3. **Click → redirect**: `GET /api/click-receiver/click/{ad_id}?user_id=u1` (no redirect
   following) → 302, `Location` == the ad's `redirect_url`, `X-Click-Status: accepted`,
   `X-Click-Id` is a uuid. `POST /api/click-receiver/clicks` → 200 with the same semantics.
4. **Dedup**: same `(user, ad)` again → 302 + `duplicate`; a different user → `accepted`.
5. **Aggregation ≤ 60 s**: 7 accepted clicks on ad A (distinct users) + 3 on ad B + 2 duplicates
   → `eventually(timeout=90s)`: the per-ad query for A totals 7, the per-advertiser query totals 10,
   `by_ad` shows A=7 and B=3, and the minute bucket(s) match the click times. Also assert the
   time from last click to visibility is < 60 s (log it).
6. **Rollups**: per-ad query with `granularity=hour` total == the `minute` total over the same range;
   a range of > 1,440 minutes with `granularity=minute` → 400.
7. **Unknown / deleted ad**: click on a non-existent id → 404, nothing counted; soft-delete an ad →
   `GET /ads` no longer lists it (click behaviour within 60 s is not asserted).
8. **Hot ad**: 1,300 accepted clicks (distinct users, ~50 concurrent) on a fresh ad →
   `eventually(timeout=30s)`: `GET /api/click-receiver/hot-ads` lists the ad with `marks ≥ 1`, and
   the next click answers `X-Click-Hot: true`; then `eventually(90s)`: the analytics total for the
   ad == the number of accepted clicks (salting + two-stage aggregation count exactly). Permanent
   marking after 10 markings is covered by unit tests (it needs 10 × 10 min in real time).

## 9. Client

- `/`: ad feed (viewer). Gets a viewer token for a random `user_id` kept in localStorage. Lists
  active ads; each ad is a plain link to `/api/click-receiver/click/{id}?user_id=…` (the real 302
  flow). A "click via API" button calls `POST /clicks` and shows `status`, `click_id` and `servedBy`.
- `/landing/[adId]`: the default landing page the demo ads redirect to ("you came from ad X").
- `/advertiser`: sign up (create advertiser) or "log in as" an id → advertiser token; manage ads
  (create with `redirect_url` defaulting to `http://localhost:8080/landing/<id>`, list, deactivate).
- `/advertiser/analytics`: per-advertiser clicks for the last 60 min (minute buckets, bar chart or
  table), per-ad totals, granularity switch, auto-refresh every 5 s, and "last updated N s ago"
  from `freshness.last_updated_at` to make eventual consistency visible. Shows 401/403/5xx
  states clearly.
- `/hot-ads`: polls `GET /api/click-receiver/hot-ads` every 2 s: hot/permanent ads, marks, expiry,
  threshold and whether salting is on (shows which receiver pod answered).

## 10. Load (→ `loadtest/ad-aggregator.js`)

Setup: 100 advertisers × 20 ads (via the API), a viewer token and advertiser tokens.

| Scenario  | Shape                                                                  | Thresholds                                                                                  |
| --------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `clicks`  | constant-arrival 500 rps for 5 min, then 1,000 rps for 1 min; `GET /click/{ad}?user_id=` with no redirect following; ads uniform, users from a 1M pool (~some dups) | p95 < 100 ms, `http_req_failed` < 0.1 %, all responses 302 |
| `hot-ad`  | same as `clicks`, but 20 % of requests go to one ad (`S=… -e SCENARIO=hot`); run twice: `HOT_SALTING_ENABLED=true` vs `false` | ad marked hot < 30 s after start; with salting, max/min per-partition message rate of `clicks` (Kafka exporter / partition offsets) < 2×, and without it the hot partition is visibly skewed (report both); p95 < 100 ms; reconciliation exact |
| `scale`   | ramp 200 → 2,000 rps over 10 min, hold 5 min                            | report: click-receiver HPA replicas, Flink parallelism over time (`kubectl get flinkdeployment -o yaml` / JM REST), consumer lag peak, analytics-db write rate. Pass = no data loss (reconciliation exact) and lag drains after the ramp; latency numbers are findings, not gates |
| `queries` | 20 rps per-advertiser and per-ad queries (last 60 min, minute)         | p95 < 500 ms, errors < 0.1 %                                                                |
| teardown  | reconciliation: sum of `accepted` counted by k6 (per advertiser) vs. analytics totals for those advertisers, polled until equal or 90 s | exact match; report reconciliation lag |

## 11. Chaos experiments (→ `chaos/`)

Each runs under the `clicks` load (reduced to 300 rps, 5 min) and ends with reconciliation
(k6 accepted == analytics total) and `make e2e`.

| # | Hypothesis                                                                                  | Fault                                                     | Verification                                                                                   |
| - | ------------------------------------------------------------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| 1 | No accepted click is lost when a Kafka broker dies; click errors < 1 %, p95 recovers < 30 s | PodChaos pod-kill of the broker leading most partitions   | reconciliation exact; error rate + p95 timeline from k6                                        |
| 2 | Killing a Flink TaskManager loses no counts; freshness recovers in < 2 min                  | pod-kill one TM                                           | reconciliation exact; job back to RUNNING (`kubectl get flinkdeployment`), lag measured         |
| 3 | Killing the Flink JobManager loses no counts (HA + S3 checkpoints)                          | pod-kill the JM                                           | reconciliation exact; restore from the latest checkpoint visible in the JM log                  |
| 4 | Redis primary loss doesn't stop clicks (fail open)                                          | pod-kill one Redis primary                                | click errors < 1 %; `click_dedup_failopen_total` > 0; over-count bounded and reported           |
| 5 | Ads-DB primary failover doesn't break redirects for cached ads                              | pod-kill `postgres` primary                               | click success ≥ 99 %; ad-placement writes fail ≤ ~30 s                                          |
| 6 | Analytics-DB outage doesn't touch the click path, and counts catch up with no loss          | pod-kill `analytics-db` primary (and a variant: NetworkChaos partition of the analytics-db pods from Flink for 60 s, `direction: both`) | zero click errors; reconciliation exact after recovery; analytics queries recover < 60 s |
| 7 | Losing a zone keeps clicks available                                                        | `chaos/node-down.sh` on one worker                        | click success ≥ 99 %; reconciliation exact                                                     |
| 8 | A Flink rescale (autoscaler or manual parallelism change 3 → 6) loses no counts             | during load, change the job's parallelism                 | reconciliation exact; job back to RUNNING; freshness gap measured                              |
| 9 | Losing Redis entirely keeps hot ads salted (last known set) and clicks flowing              | pod-kill all Redis pods during `hot-ad` load              | click errors < 1 %; hot ad still salted (`X-Click-Hot: true`); markings resume after recovery  |

## 12. Open questions / assumptions

- **Redis = dedup window (10 min)**, fail open **(decided)**. Uniqueness is only guaranteed within
  10 min and while Redis is healthy; exact "unique forever" would need Flink keyed state or a DB
  constraint on raw clicks (not built).
- **Two Postgres clusters (decided)** via the new `instance:` support in `stack.yaml`.
- **Floci S3 for Flink checkpoints + JM HA (decided).**
- **JWT at Envoy Gateway (decided)**; `auth` is a demo token issuer with no passwords, and it
  doesn't validate that the advertiser exists.
- The diagram's `User` table isn't built: `user_id` is an opaque string (from the viewer token /
  query param). The `Click` entity exists only as an event (no raw-click table).
- The diagram's `ClickCount.id` surrogate key is replaced by the composite key `(ad_id, minute)`,
  and `advertiser_id` is denormalized into it (needed for idempotent upserts and per-advertiser
  queries without a cross-DB join). The event carries `advertiser_id` from the receiver's ad lookup.
- "Aggregated data is de-aggregated and stored" (data flow step 4) is read as "per-minute
  aggregates are stored and rolled up at query time".
- A click during a Kafka outage returns 503 rather than redirecting: the loss is visible, never
  silent.
- Hot-ad salting **(decided)**: threshold 1,200 accepted clicks per 10 min (lab value; the uniform
  load gives ~150/ad/10 min, so only the skewed ad crosses it), hot for 10 min per marking,
  permanent after 10 markings. The sliding window is minute-granular (approximate). Per-ad ordering
  in Kafka is lost for hot ads (irrelevant for counting).
- Autoscaling is capped by the laptop: at most 12 TMs × ~1 GiB and 9 receivers.
