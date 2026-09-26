# redis (Redis 8, standalone or Redis Cluster)

Official `redis:8.8.3` image in a StatefulSet (`data/redis`, pods `redis-N`), plus an
`oliver006/redis_exporter:v1.92.0` sidecar. No operator and no Bitnami: `install.sh` bootstraps the
cluster itself so the zone layout is under our control (see PLAYBOOK.md for why).

One instance per design (`install.sh <profile> [redis]`; any other instance name fails fast).

## Profiles

| Profile | Pods | `MODE`       | Layout                                                                                        |
| ------- | ---- | ------------ | --------------------------------------------------------------------------------------------- |
| `small` | 1    | `standalone` | single node                                                                                   |
| `ha`    | 6    | `cluster`    | Redis Cluster: 3 primaries (one per zone) + 3 replicas, each replica in a different zone from its primary |

Each pod: 50m / 128Mi request, 384Mi limit, `maxmemory 256mb` + `volatile-lru` (only keys with a TTL
are evicted), AOF `everysec`, 1Gi PVC. PDB `maxUnavailable: 1` on `ha`.

How `ha` works:

- Every node announces its stable FQDN (`cluster-announce-hostname
  redis-N.redis-headless.data.svc.cluster.local`, `cluster-preferred-endpoint-type hostname`), so
  `MOVED`/`CLUSTER SLOTS`/`CLUSTER SHARDS` hand clients hostnames, not pod IPs.
- On start, `start.sh` rewrites the IPs in `nodes.conf` from those FQDNs (headless service with
  `publishNotReadyAddresses`), so the cluster re-forms even if **all** pods restart at once.
- `cluster-node-timeout 5000` → a replica is promoted ~5–10 s after its primary dies.
  `cluster-require-full-coverage no` → the other shards keep serving while one fails over.
- `install.sh` creates the cluster once (primaries = one pod per zone, replicas attached across
  zones with `--cluster add-node --cluster-slave`); on re-runs it sees 6 known nodes and skips it.

## Connection contract — Secret `apps/redis-conn`

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

Reset everything: `infra/components/redis/uninstall.sh` (deletes the PVCs) then `make up`.

## Smoke test

`smoke.sh`: from namespace `apps` via `redis-conn`, SET/GET 30 keys (`-c`, following redirects),
MSET/MGET and a Lua script on `{hash-tag}` keys; on `ha` checks `cluster_state:ok` + all 16384
slots, that the 30 keys are spread over all 3 primaries, and that each primary has a replica with
`master_link_status:up` that acknowledges `WAIT 1` (warns if a replica shares its primary's zone).

## Experiments

- Kill one primary (PodChaos on the pod whose `redis-cli role` is `master`): its replica is promoted
  in ~5–10 s; the killed pod rejoins as a replica. Clients see errors for that shard only.
- Kill all six pods at once: the cluster re-forms from `nodes.conf` + AOF (hostnames → new IPs).
- `chaos/node-down.sh` on a zone: one primary + one replica are lost; the replica of the lost
  primary lives in another zone and takes over.
