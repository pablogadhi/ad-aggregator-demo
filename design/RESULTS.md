# Ad Click Aggregator — results

Built from `design/diagram.excalidraw` with `/build-design` (spec: `design/spec.md`, contracts:
`design/contracts/`). Everything below was measured on the local 5-node kind cluster (3 zones,
Docker Desktop 32 CPU / 47 GiB) over three integration rounds (2026-09-26 → 29). Round 3 (r3) runs
on the final stack: every component from Artifact Hub charts, installed and reconciled by **Flux**,
load generated **in-cluster** by the k6-operator.

## What was built

```
browser ──302── click-receiver ×4–18 ──acks=all──▶ Kafka `clicks` (12 part, RF3) ──▶ Flink click-aggregator (3–12)
   │              │  dedup + hot-ad state                                               │ two-stage count, S3 checkpoints
   │              └──▶ Redis Cluster 3+3                                               ▼
   │                                                              analytics-db (CNPG ×3) ◀── analytics ×2
   └── Envoy Gateway ×3 (JWT on ad-placement/analytics) ── auth ×2, ad-placement ×2 ──▶ postgres (CNPG ×3)
```

| Layer      | What |
| ---------- | ---- |
| Delivery   | **Flux** (Flux Operator 0.60.0 + `FluxInstance`) syncs `infra/` from an **OCI artifact in the kind registry** (no Git remote): platform → components (one Kustomization per `stack.yaml` entry) → design, ordered by `dependsOn`, with drift correction. Web UI `flux.localhost:8080`. Services, pipeline and client stay on Tilt (inner dev loop). |
| Components | All from pinned **Artifact Hub charts**, glue via `bedag/raw` only for `lookup`-generated credentials: `cnpg/cluster` 0.8.1 ×2 (`postgres`, `analytics-db`) on CloudNativePG 1.30, Strimzi 1.2.0 (Kafka 4.3.1 CRs, 3 brokers, one per zone) + kafbat Kafka UI 1.6.5, CloudPirates `redis` 0.35.4 (Redis 8.10.2 Cluster 3+3, init container repairs peer IPs), quench `floci` 0.2.18 (S3 for Flink checkpoints/HA), Flink operator 1.16.1 (job on Flink 2.2.1) |
| Platform   | Envoy Gateway 1.9.1 (controller ×2, **proxies ×3, one per zone**), cert-manager, metrics-server, kube-prometheus-stack, Chaos Mesh, **Grafana k6-operator** 4.6.0 |
| Services   | `auth` (demo JWT issuer + JWKS, fetched by the gateway), `ad-placement` (advertiser/ad CRUD, ad feed), `click-receiver` (hot path), `analytics` (minute/hour/day rollups) |
| Pipeline   | `click-aggregator`: Flink SQL, `(ad, salt, minute)` → `(ad, minute)` continuous count, JDBC upsert, exactly-once state (10 s checkpoints to S3), Kubernetes HA, operator autoscaler 3–12 |
| Gateway    | JWT `SecurityPolicy` (remote JWKS from `auth`) with claims → `X-Auth-*` headers, 2 s timeout / no retries on clicks, outlier detection on every app route |
| Client     | Next.js: ad feed `/`, `/landing/[adId]`, `/advertiser`, `/advertiser/analytics` (live, freshness), `/hot-ads` |

Click path: ad lookup (in-process cache → read replica → primary) → dedup `SET NX EX 600` (fails
open, per-node circuit breaker) → salt key if the ad is hot → produce with `acks=all` and **wait for
the ack** (1 s deadline, load shedding) → 302. Hot ads: batched per-minute counters in Redis, marked
hot at ≥ 1,200 clicks / 10 min, permanent after 10 markings, key salted over 12 partitions.

## How to run

```bash
make up                     # kind cluster + Flux; Flux installs platform, components, design (~9 min cold)
make ci                     # Tilt: build + deploy services, pipeline, client; run the 27 e2e tests
make sync                   # after editing infra/ or stack.yaml: push to Flux and wait for reconcile
make load S=clicks          # in-cluster k6 (3 runners, one per zone) + aggregated gates + reconcile
make chaos E=<experiment>   # see chaos/README.md; make chaos-clear afterwards
```

App `http://localhost:8080` (APIs at `/api/{auth,ad-placement,click-receiver,analytics}`), Flux UI
`flux.localhost:8080`, Kafka UI `kafka-ui.localhost:8080`, Grafana `grafana.localhost:8080`
(admin/admin), Prometheus `prometheus.localhost:8080`, Chaos Mesh `chaos.localhost:8080`.

**Reconciliation** (the core check): k6 counts `X-Click-Status: accepted` per advertiser (as a
Prometheus counter); analytics must show **lost = 0** and **over-count ≤ ambiguous responses**
(503s/timeouts whose Kafka write may have landed — see spec §5.1).

## Results

**e2e: 27/27** (r3, three times: after each chaos run and at the end). Click → visible in analytics
in 0.5–1.0 s. Hot ad: 1,300 clicks, listed hot ~1 s later, every receiver salting within ~1 s,
counted exactly.

### Load

| Run | p95 | Errors | Reconcile | Verdict |
| --- | --- | ------ | --------- | ------- |
| `clicks` in-cluster, 3 runners (r3): 500 rps × 5 min + 1,000 rps × 1 min | 11.9 ms steady / 16.9 ms burst | 0 % (210,001) | exact 209,999, lag 15.3 s | PASS |
| `clicks` host k6, cold burst (r2) | 16.6 ms burst | 0 % | exact, lag 10.8 s | PASS |
| `scale` 200 → 2,000 rps + 5 min hold (r2) | 43.5 ms @ 1,700 rps; 90.3 ms @ 2,000 hold | 7 × 503 | lost 0, ambiguous 7 | PASS* |
| `hot-ad` 20 % on one ad, salting on / off (r1) | 18.8 / 19.0 ms | 1 / 0 | exact / exact | PASS |
| analytics queries 20 rps | 5.7 ms (r3); 10.8–15.8 ms (r1) | 0 | — | PASS |

\* 10-second buckets during the 2,000 rps hold still spiked to p95 229–316 ms; HPA peaked at 15/18
pods; dedup fail-open 0.9 %. Hot-ad salting: per-partition rate max/min **1.59×** with salting vs
**4.35×** without. Round 1 → 2 on the click path: burst p95 398 → 17 ms (warm-up + min 4 pods),
2,000 rps p95 525 → 90 ms (≈ 2× per-core throughput), over-count from gateway 504s +536 → 0
(1 s deadline + load shedding).

### Chaos (300 rps, fault ~60 s in, then reconcile + e2e)

| #   | Fault | Click impact | Recovery | Reconcile |
| --- | ----- | ------------ | -------- | --------- |
| 1   | Kafka broker kill (leader of 4/12) — r1 | 0 errors | p95 104 ms for one 10 s bucket | exact |
| 2   | Flink TaskManager kill — r1 | 0 errors | RUNNING from checkpoint in 11.6 s | exact |
| 3   | Flink JobManager kill — r1 | 0 errors | HA restore, RUNNING in 61 s (≈ 65 s aggregation pause) | exact |
| 4   | Redis primary kill — r2 | 0.005 % errors | restarted as primary before failover kicked in | exact |
| 5   | Ads-DB primary kill — r1 | 99.992 % OK | writes failed ~25 s | exact |
| 6a  | analytics-db primary kill — r1 | 0 errors | queries back < 10 s | exact |
| 6b  | Flink ↔ analytics-db partition 60 s — r1 | 0 errors | checkpoint stalled 106 s; sink caught up | exact |
| 7   | **Zone-b node down** ~2 min 15 s — r3 | **99.976 %** OK (26 of 107,977) | ~15 s error window (≈ the 20 s node-monitor grace); a Redis primary on zone-b failed over and receivers followed (dedup fail-open ~1.2 %); the old primary rejoined as a replica | lost 0, over 1 ≤ ambiguous 26 |
| 8   | Flink rescale 3 → 6 → 3 under load — r2 | 99.98 % OK (18 × 503) | in-place rescale (r1: full redeploy, 2 min 37 s gap) | lost 0, over 18 = ambiguous 18 |
| 9   | **All 6 Redis pods killed** under hot load — r3 | **0 errors** (90,001) | re-formed **by itself**: `cluster_state:ok` in ~1 min 30 s, full 3+3 in ~2 min 40 s; receivers stayed Ready, 0 restarts | exact 89,977 |
| 10  | **Drift** (r3): delete `apps/kafka-conn` + the `kafka-ui` Deployment under load | 0.004 % (1 of 24,001) | Flux recreated both within seconds of a reconcile (natural interval ≤ 2 min / ≤ 5 min); `flux suspend` stops reverts, `resume` restores them | lost 0 |

How #7 and #9 got here: in r3's first attempt, the CloudPirates Redis chart never re-formed after a
kill-all (the cluster bus reconnects by IP; hostname announce only affects client redirects), the
click receivers then starved their event loop on redis-py's re-initialize storm, and a zone loss sent
~10 % of gateway traffic to a dead Envoy proxy. Fixes: an init container that rewrites peer IPs in
`nodes.conf`, a single-attempt Redis client with a circuit breaker and backoff (loop lag under
`CLUSTERDOWN` 80–113 → 4–28 ms), Envoy proxies ×3 across zones, and `node-monitor-grace-period: 20s`.
Round 2's 100 % on #7 was partly luck: no Envoy proxy happened to run in zone-b.

Flink JobManager after the r2 runs: 1,475 Mi of 2 Gi, 0 restarts (at 1 Gi: OOMKilled after 41 min).

## NFRs

| NFR | Held? | Evidence / why |
| --- | ----- | -------------- |
| High availability | Yes | zone loss 99.976 % clicks OK (~15 s error window); all Redis down → 0 errors; broker/DB/Flink kills ≤ 0.01 % errors |
| No clicks lost on failure | Yes\* | lost = 0 in every load and chaos run. \*A 503 after an in-flight Kafka timeout may still be counted (over-count ≤ ambiguous, e.g. 18/18 in #8). A **Docker Desktop hard stop** can corrupt Flink's HA checkpoint in Floci (see below) |
| Click + redirect < 100 ms | Mostly | p95 12–17 ms at 500–1,000 rps, 90 ms at 2,000 rps; 10 s tail spikes to ~300 ms during the 2,000 rps hold |
| Metrics query < 500 ms | Yes | p95 6–16 ms |
| Eventual consistency ≤ 1 min | Yes, except during Flink recovery | 0.5–1 s normally; JM kill ≈ 65 s, analytics-db partition 106 s, rescale gap ~65 s |
| Scale (lab: 500 rps, 1,000 burst, 2,000 ramp) | Yes | 2,000 rps with 15 receivers; Flink stayed at 3 (lag never exceeded ~500 records, so the autoscaler didn't need to act) |

## Known issues / findings

1. **Hard stop of Docker Desktop** (quit/reboot with the lab running) isn't survived cleanly: Floci
   can keep a 0-byte Flink HA checkpoint object (job stuck in `INITIALIZING`; recovery:
   `pipelines/click-aggregator/reset.sh`, loses open-minute state), and Redis AOF tails can be
   corrupt (tolerated via `aof-load-corrupt-tail-max-size`). Prefer `make down` before quitting
   Docker. The first Maven request after Docker starts often fails; the pipeline build retries it.
2. **Redis replica placement isn't zone-guaranteed** with the CloudPirates chart
   (`redis-cli --cluster create` assigns replicas); `make smoke` warns if a primary and its replica
   share a zone. Full re-form after a kill-all takes ~2 min 40 s because the StatefulSet starts pods
   one at a time (`OrderedReady`); `podManagementPolicy: Parallel` would cut it to ~50 s.
3. **Tail latency at 2,000 rps** (10 s p95 up to ~300 ms) — next suspects: Redis dedup or Kafka
   produce latency under peak, HPA adding pods, GC.
4. **`make ci` stalled on the Flink pipeline** once, after its FlinkDeployment was deleted by hand
   (Tilt never re-applied it). Not seen on fresh clusters since.
5. **Flux substitution:** Flux's `postBuild` envsubst parses every dollar-brace in values files,
   comments included. Shell scripts live in ConfigMaps annotated
   `kustomize.toolkit.fluxcd.io/substitute: disabled`.
6. **In-cluster k6:** `setup()` runs once per runner (a 3-runner run creates 300 advertisers, not
   100; total load is still exact), and the aggregated p95 is the max of the runners' p95s (no
   native histograms in this Prometheus).
7. **Sandbox tooling:** from Claude Code, bare `kubectl` and `curl localhost:8080` run sandboxed;
   use `docker exec sdl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf …`. Pass
   options to make as arguments (`make load S=chaos HOT=1`), not as env-var prefixes.
8. `chaos/redis-primary-kill.yaml` names a pod: re-check `redis-cli cluster nodes` before running it.

## Next experiments

- **Client-supplied click ids + dedup on `click_id` in Flink** — removes the ambiguous over-count
  (exactly-once end to end, not just "lost = 0").
- **Batch reconciliation path** (raw clicks → S3 → periodic recount) for late events > 1 h and for
  the hard-stop case.
- Push past 2,000 rps to make the **Flink autoscaler** act, and find the analytics-db write ceiling
  (~1,200 row updates/s seen).
- Redis `podManagementPolicy: Parallel` and zone-aware replica assignment.
- Synchronous replication on analytics-db: cost vs. no lost aggregates on failover.
- JWT key rotation (two `kid`s in the JWKS) without downtime.
