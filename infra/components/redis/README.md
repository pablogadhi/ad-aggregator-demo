# redis (Redis 8, standalone or Redis Cluster)

CloudPirates `redis` chart **0.35.4** (OCI `oci://registry-1.docker.io/cloudpirates/redis`, Artifact Hub),
release = instance (default `redis`) in namespace `data`. It runs the official `docker.io/redis:8.10.2`
and `oliver006/redis_exporter:v1.91.1` images, both digest-pinned by the chart. No operator, no Bitnami,
no local chart code: `values/small.yaml` / `values/ha.yaml` are chart values, and the conn secret comes
from the chart's `extraObjects`.

Flux base `flux/instance/` (Kustomization `<instance>` per `stack.yaml` entry): `OCIRepository <instance>-chart`,
ConfigMap `<instance>-values` (both profiles), HelmRelease `<instance>` with `valuesKey: ${PROFILE}.yaml`.
Instance `redis` → StatefulSet/Service `redis`, pods `redis-N`,
headless `redis-headless`, secret `apps/redis-conn`. Another instance `cache` → `cache-redis-N`,
`apps/cache-conn` (the chart prefixes the release name unless it contains `redis`).

## Profiles

| Profile | Pods | `MODE`       | Layout                                                                         |
| ------- | ---- | ------------ | ------------------------------------------------------------------------------ |
| `small` | 1    | `standalone` | single node                                                                    |
| `ha`    | 6    | `cluster`    | Redis Cluster: 3 primaries + 3 replicas, pods spread 2 per zone (topologySpreadConstraints) |

Each pod: 50m / 128Mi request, 384Mi limit, `maxmemory 256mb` + `volatile-lru` (only keys with a TTL
are evicted), AOF `everysec` with `aof-load-corrupt-tail-max-size 1048576` (survives a torn AOF tail
after a hard stop), 1Gi PVC (deleted on `helm uninstall`). PDB `maxUnavailable: 1` on `ha`. Metrics via
the exporter sidecar + ServiceMonitor `<fullname>-metrics`.

How `ha` works:

- The chart's post-install/post-upgrade Job (`<fullname>-init-cluster`) runs `redis-cli --cluster create
  --cluster-replicas 1` once all 6 pods answer. It skips a healthy cluster and, when nodes already hold
  state, waits for them to re-form instead of recreating (so `make up` re-runs are no-ops).
- `cluster.announceHostnames: true`: every node announces `redis-N.redis-headless.data.svc.cluster.local`
  (`cluster-preferred-endpoint-type hostname`), so `MOVED`/`CLUSTER SLOTS` hand clients hostnames and
  `nodes.conf` holds hostnames — the cluster re-forms by itself even after **all** pods restart with new IPs.
- `cluster-node-timeout 5000` → a replica is promoted ~5–10 s after its primary dies.
  `cluster-require-full-coverage no` → the other shards keep serving while one fails over.
- **Trade-off (accepted):** `--cluster create` is not zone-aware, so a primary and its replica can share
  a zone. Losing that zone then loses that shard until the zone returns (dedup/hot-ad keys of that shard
  fail open; clicks are unaffected). `smoke.sh` reports such pairs as a warning, not a failure.

## Connection contract — Secret `apps/<instance>-conn` (from `extraObjects`)

| Key    | Env (with `connections: [redis]`) | Value                                                  |
| ------ | --------------------------------- | ------------------------------------------------------ |
| `URL`  | `REDIS_URL`                       | `redis://redis.data.svc.cluster.local:6379`            |
| `MODE` | `REDIS_MODE`                      | `cluster` (ha) / `standalone` (small)                  |
| `HOST` | `REDIS_HOST`                      | `redis.data.svc.cluster.local` (ClusterIP over all nodes) |
| `PORT` | `REDIS_PORT`                      | `6379`                                                 |

No password. Client by mode (Python):

```python
if settings.mode == "cluster":
    r = redis.asyncio.RedisCluster.from_url(settings.url)   # seed = the service, then follows the slot map
else:
    r = redis.asyncio.Redis.from_url(settings.url)
```

In cluster mode multi-key commands, transactions and Lua scripts need all their keys in one slot:
use a hash tag (`hot:{a:42}:flag`, `hot:{a:42}:marks`).

## Operating it

```bash
kubectl -n data exec -it redis-0 -c redis -- redis-cli cluster nodes      # roles, slots, links
kubectl -n data exec -it redis-0 -c redis -- redis-cli -c get somekey     # -c follows MOVED
kubectl -n data exec -it redis-0 -c redis -- redis-cli --cluster check localhost:6379
```

Reset everything: remove the entry from `stack.yaml`, `make sync` (Flux uninstalls the release; the PVC
retention policy deletes the PVCs), then put it back and `make sync` again.

## Smoke test

`smoke.sh [instance]`: from namespace `apps` via `<instance>-conn`, SET/GET 30 keys (`-c`, following redirects),
MSET/MGET and a Lua script on `{hash-tag}` keys; on `ha` checks `cluster_state:ok` + all 16384
slots, that the 30 keys are spread over all 3 primaries, and that each primary has a replica with
`master_link_status:up` that acknowledges `WAIT 1` (pairs read from `CLUSTER NODES`), and reports the
primary/replica pairs that share a zone (warning only).

## Experiments

- Kill one primary (PodChaos on the pod whose `redis-cli role` is `master`): its replica is promoted
  in ~5–10 s; the killed pod rejoins as a replica. Clients see errors for that shard only.
- Kill all six pods at once: the cluster re-forms from `nodes.conf` + AOF (announced hostnames → new IPs),
  no init Job or script involved.
- `chaos/node-down.sh` on a zone: two pods are lost. A primary whose replica is in another zone fails
  over; a same-zone pair (see `smoke.sh`) leaves its slots unserved until the zone returns.
