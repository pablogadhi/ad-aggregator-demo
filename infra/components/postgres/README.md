# postgres (CloudNativePG)

PostgreSQL managed by the CloudNativePG operator (chart 0.29.0 / operator 1.30.0; the operator picks
the Postgres image, currently 18.x). This is the **reference component** — see `../AUTHORING.md`.

## Profiles

| Profile | Instances                                                | Use                                |
| ------- | -------------------------------------------------------- | ---------------------------------- |
| `small` | 1                                                        | fast, no replicas, no failover     |
| `ha`    | 3 (1 primary + 2 async streaming replicas, one per zone) | failover / replica-lag experiments |

## Instances

`install.sh <profile> [instance]` — the instance (default `postgres`) names the CNPG `Cluster`
(pods `<instance>-N`, services `<instance>-rw` / `<instance>-ro`), its PodMonitor and the conn secret
`apps/<instance>-conn`. List the component several times in `stack.yaml` for independent databases:

```yaml
components:
  - { name: postgres, profile: ha }                          # -> postgres-conn
  - { name: postgres, instance: analytics-db, profile: ha }  # -> analytics-db-conn (ANALYTICS_DB_URL, …)
```

`smoke.sh [instance]` and `uninstall.sh [instance]` take the same argument. Every instance uses
database `app`, owner `app`.

## Connection contract — Secret `apps/<instance>-conn`

Shown for instance `postgres`; for another instance replace the prefix (`analytics-db` →
`ANALYTICS_DB_URL`, hosts `analytics-db-rw/-ro.data.svc.cluster.local`).

| Key                 | Env (with `connections: [postgres]`)  | Value                                                            |
| ------------------- | ------------------------------------- | ---------------------------------------------------------------- |
| `HOST`              | `POSTGRES_HOST`                       | `postgres-rw.data.svc.cluster.local` (always the primary)        |
| `READ_HOST`         | `POSTGRES_READ_HOST`                  | `postgres-ro.data.svc.cluster.local` (replicas only)             |
| `PORT`              | `POSTGRES_PORT`                       | `5432`                                                           |
| `USER` / `PASSWORD` | `POSTGRES_USER` / `POSTGRES_PASSWORD` | app owner credentials (generated)                                |
| `DATABASE`          | `POSTGRES_DATABASE`                   | `app`                                                            |
| `URL` / `READ_URL`  | `POSTGRES_URL` / `POSTGRES_READ_URL`  | `postgresql://…` DSNs for the above                              |
| `JDBC_URL`          | `POSTGRES_JDBC_URL`                   | `jdbc:postgresql://<HOST>:5432/app` — no credentials (use `USER`/`PASSWORD`); for Flink/JVM clients |

Python: `sdl_common.postgres.Database(PostgresSettings())` gives `primary` and `replica` pools.
Schema changes: SQL files shipped in the service package, applied by `sdl_common.migrate` in an init
container (advisory-locked, so N replicas can start at once).

Need more databases (e.g. one per service, or Temporal's)? Add a CNPG `Database` CR in the design's
`infra/design/` and extend the conn secret (or create `<name>-conn` for it) — don't change this component.

## Operating it

```bash
kubectl -n data get cluster                                  # every instance: status, current primary
kubectl -n data get pods -L cnpg.io/instanceRole             # who is primary
kubectl -n data exec -it postgres-1 -c postgres -- psql app  # psql as superuser
```

## Experiments

- `make chaos E=postgres-primary-kill` under `make load`: failover ~20s; writes fail/queue during it.
- Replica lag: write then `GET /items?source=replica` immediately (sample-api) — sometimes missing.
- Synchronous replication (`spec.postgresql.synchronous: {method: any, number: 1}`): no lost commits on
  failover, but higher write latency — measure the difference with k6.
- `chaos/node-down.sh <node-of-primary>`: the instance can't move (local-path volume); CNPG fails over,
  the old primary rejoins as a replica when the node returns.
