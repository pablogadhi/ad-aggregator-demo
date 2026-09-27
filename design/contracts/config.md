# Configuration matrix

Which connection contracts and env vars each workload gets (written by the architect).
`connections:` entries become env vars with a prefix: `postgres` -> `POSTGRES_URL`,
`analytics-db` -> `ANALYTICS_DB_URL` (dash → underscore), ... (keys: infra/components/AUTHORING.md).

## Connection secrets (all in namespace `apps`, label `sdl.dev/conn: "true"`)

| Secret              | Published by                                  | Keys                                                                                   |
| ------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------- |
| `postgres-conn`     | postgres component, instance `postgres`       | `HOST READ_HOST PORT USER PASSWORD DATABASE URL READ_URL JDBC_URL`                     |
| `analytics-db-conn` | postgres component, instance `analytics-db`   | same keys; hosts `analytics-db-rw/-ro.data.svc.cluster.local`, database `app`          |
| `kafka-conn`        | kafka component                               | `BOOTSTRAP_SERVERS`                                                                    |
| `redis-conn`        | redis component                               | `URL` (`redis://…`), `MODE` (`cluster` for ha, `standalone` for small), `HOST`, `PORT` |
| `aws-conn`          | aws component (Floci)                         | `ENDPOINT_URL`, `REGION`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`                         |
| `flink-conn`        | flink component                               | `REST_URL`                                                                             |
| `jwt-conn`          | `infra/design/` (keypair generated at `make up`, stable across re-runs) | `PRIVATE_KEY_PEM`, `PUBLIC_KEY_PEM`, `KID`, `ISSUER` (`ad-aggregator-auth`), `AUDIENCE` (`ad-aggregator`) |

`JDBC_URL` = `jdbc:postgresql://<HOST>:5432/app` (no credentials; pass `USER`/`PASSWORD` separately).

## Workloads

| Workload                     | connections                      | extra env                                                                                                  | route                                          |
| ---------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| auth                         | `jwt`                            | `TOKEN_TTL_SECONDS=3600`                                                                                   | `/api/auth` (no JWT)                           |
| ad-placement                 | `postgres`                       | `CLICK_URL_PREFIX=/api/click-receiver/click` (migrations on)                                               | `/api/ad-placement` (JWT)                      |
| click-receiver               | `postgres`, `kafka`, `redis`     | `CLICKS_TOPIC=clicks`, `DEDUP_TTL_SECONDS=600`, `AD_CACHE_TTL_SECONDS=60`, `REDIS_TIMEOUT_MS=50`, `KAFKA_DELIVERY_TIMEOUT_MS=1500`, `HOT_THRESHOLD_CLICKS_10M=1200`, `HOT_MARK_TTL_SECONDS=600`, `HOT_PERMANENT_AFTER_MARKS=10`, `HOT_SALT_BUCKETS=12`, `HOT_SALTING_ENABLED=true`, `HOT_FLUSH_INTERVAL_MS=1000`, `HOT_REFRESH_INTERVAL_MS=2000`, `CLICK_DEADLINE_MS=1000`. Chart: `autoscaling.enabled: true`, min 4 / max 18 | `/api/click-receiver` (no JWT, timeout 2 s) |
| analytics                    | `analytics-db`                   | `MAX_BUCKETS=1440` (migrations on)                                                                         | `/api/analytics` (JWT)                         |
| click-aggregator (Flink job) | `kafka`, `analytics-db`, `aws` (env from the secrets in its FlinkDeployment pod template) | `CLICKS_TOPIC=clicks`, `CONSUMER_GROUP=click-aggregator`, `STATE_BUCKET=flink-state`, `CHECKPOINT_INTERVAL=10s`, `STATE_TTL=2h`, `LATE_CUTOFF=1h`; autoscaler `job.autoscaler.enabled=true`, parallelism min 3 / max 12, `pipeline.max-parallelism=120`, TMs 1 slot each | — |
| client                       | —                                | —                                                                                                          | `/`                                            |

## Gateway policies (`infra/design/`)

| Target HTTPRoute (ns `apps`) | Policy                                                                                                   |
| ---------------------------- | -------------------------------------------------------------------------------------------------------- |
| `ad-placement`, `analytics`  | `SecurityPolicy` JWT (RS256, iss/aud above), `claimToHeaders`: `sub`→`X-Auth-Sub`, `role`→`X-Auth-Role`, `advertiser_id`→`X-Auth-Advertiser-Id` |
| `click-receiver`             | `BackendTrafficPolicy`: timeout 2 s, no retries                                                          |
| `auth`, `client`             | none                                                                                                     |
