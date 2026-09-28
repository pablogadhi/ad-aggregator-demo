# Ad Click Aggregator — results

Built from `design/diagram.excalidraw` with `/build-design` (spec: `design/spec.md`, contracts:
`design/contracts/`). Everything below was measured on the local 5-node kind cluster (3 zones,
Docker Desktop 32 CPU / 47 GiB), rounds 1 and 2 of integration (2026-09-26 → 27).

## What was built

```
browser ──302── click-receiver ×4–18 ──acks=all──▶ Kafka `clicks` (12 part, RF3) ──▶ Flink click-aggregator (3–12)
   │              │  dedup + hot-ad state                                               │ two-stage count, S3 checkpoints
   │              └──▶ Redis Cluster 3+3                                               ▼
   │                                                              analytics-db (CNPG ×3) ◀── analytics ×2
   └── Envoy Gateway (JWT on ad-placement/analytics) ── auth ×2, ad-placement ×2 ──▶ postgres (CNPG ×3)
```

| Layer      | What                                                                                                                                                                                                                                                   |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Components | `postgres` ×2 instances (`postgres` = ads, `analytics-db`), Kafka 4.3.1 (Strimzi 1.2.0, 3 brokers, one per zone), Redis 8.8.3 Cluster (3 primaries + 3 replicas), Floci S3 (Flink checkpoints/HA), Flink operator 1.16.1 (job on Flink 2.2.1) |
| Services   | `auth` (demo JWT issuer + JWKS), `ad-placement` (advertiser/ad CRUD, ad feed), `click-receiver` (hot path), `analytics` (minute/hour/day rollups)                                                                                                       |
| Pipeline   | `click-aggregator`: Flink SQL, `(ad, salt, minute)` → `(ad, minute)` continuous count, JDBC upsert, exactly-once state (10 s checkpoints to S3), Kubernetes HA, operator autoscaler 3–12                                                              |
| Gateway    | Envoy Gateway: JWT `SecurityPolicy` with claims → `X-Auth-*` headers, 2 s timeout / no retries on clicks, outlier detection on every app route, controller ×2 across zones                                                                            |
| Client     | Next.js: ad feed `/`, `/landing/[adId]`, `/advertiser`, `/advertiser/analytics` (live, freshness), `/hot-ads`                                                                                                                                         |

Click path: ad lookup (in-process cache → read replica → primary) → dedup `SET NX EX 600` (fails
open) → salt key if the ad is hot → produce with `acks=all` and **wait for the ack** → 302. Hot ads:
batched per-minute counters in Redis, marked hot at ≥ 1,200 clicks / 10 min, permanent after 10
markings, key salted over 12 partitions.

## How to run

```bash
make up && make ci          # cluster + components, then build/deploy/e2e (see "Known issues" for make ci)
open http://localhost:8080  # client; APIs at /api/{auth,ad-placement,click-receiver,analytics}
make e2e                    # 27 acceptance tests
k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=clicks -e RUN=clicks loadtest/ad-aggregator.js
k6 run -e BASE_URL=http://localhost:8080 -e RUN=clicks loadtest/reconcile.js
```

Grafana `grafana.localhost:8080` (admin/admin), Prometheus `prometheus.localhost:8080`, Chaos
`chaos.localhost:8080`. Load/chaos procedures: `loadtest/README.md`, `chaos/README.md`.

**Reconciliation** (the core check): k6 counts `X-Click-Status: accepted` per advertiser; analytics
must show **lost = 0** and **over-count ≤ ambiguous responses** (503s/timeouts whose Kafka write may
have landed — see spec §5.1).

## Results

**e2e: 27/27.** Click → visible in analytics in 0.5–2.1 s. Hot ad: 1,300 clicks, listed hot 0.5–2 s
later, every receiver salting within 0.7–1.7 s, counted exactly.

### Load

| Run                                     | p95                                       | Errors            | Reconcile                                   | Verdict |
| --------------------------------------- | ----------------------------------------- | ----------------- | ------------------------------------------- | ------- |
| `clicks` 500 rps × 5 min, cold burst 1,000 rps × 1 min (round 2) | 16.6 ms (burst)                           | 0 %               | exact, lag 10.8 s                           | PASS    |
| `scale` 200 → 2,000 rps + 5 min hold (round 2) | 43.5 ms @ 1,700 rps; 90.3 ms @ 2,000 hold | 7 × 503           | lost 0, ambiguous 7, lag 205 s              | PASS*   |
| `hot-ad` 20 % on one ad, salting on / off (round 1) | 18.8 / 19.0 ms                            | 1 / 0             | exact / exact                               | PASS    |
| analytics queries 20 rps (round 1)      | 10.8–15.8 ms                              | 0                 | —                                           | PASS    |

\* 10-second buckets during the 2,000 rps hold still spiked to p95 229–316 ms (max 818 ms); HPA
peaked at 15/18 pods; dedup fail-open 0.9 % (round 1: ≈ 100 %). Hot-ad salting: per-partition
rate max/min **1.59×** with salting vs **4.35×** without; ad marked hot 12.7 s into the run.

Round 1 → round 2 on the click path: burst p95 398 → 17 ms (warm-up + min 4 pods), 2,000 rps p95
525 → 90 ms (≈ 2× per-core throughput from an ASGI middleware rewrite + 1 % access-log sampling,
plus HPA max 9 → 18), over-count from gateway 504s +536 → 0 (1 s deadline + load shedding).

### Chaos (300 rps, fault ~60 s in, then reconcile + e2e)

| #   | Fault                                          | Click impact                     | Recovery                                                       | Reconcile                    |
| --- | ---------------------------------------------- | -------------------------------- | -------------------------------------------------------------- | ---------------------------- |
| 1   | Kafka broker kill (leader of 4/12) — r1        | 0 errors                         | p95 104 ms for one 10 s bucket                                 | exact                        |
| 2   | Flink TaskManager kill — r1                    | 0 errors                         | RUNNING from checkpoint in 11.6 s                              | exact                        |
| 3   | Flink JobManager kill — r1                     | 0 errors                         | HA restore, RUNNING in 61 s (≈ 65 s aggregation pause)         | exact                        |
| 4   | Redis primary kill — r2                        | 0.005 % errors                   | restarted as primary before failover kicked in                 | exact                        |
| 5   | Ads-DB primary kill — r1                       | 99.992 % OK                      | writes failed ~25 s                                            | exact                        |
| 6a  | analytics-db primary kill — r1                 | 0 errors                         | queries back < 10 s                                            | exact                        |
| 6b  | Flink ↔ analytics-db partition 60 s — r1       | 0 errors                         | checkpoint stalled 106 s; sink caught up                       | exact                        |
| 7   | Zone-b node down — r2                          | **100 %** OK                     | gateway: no stale window (r1: 88 % failures for ~5 min)        | exact (54,000)               |
| 8   | Flink rescale 3 → 6 → 3 under load — r2        | 99.98 % OK (18 × 503)            | in-place rescale (r1: full redeploy, 2 min 37 s gap)           | lost 0, over 18 = ambiguous 18 |
| 9   | **All 6 Redis pods killed** under hot load — r2 | 0 errors                         | cluster re-formed 3+3 in ~5–10 s; hot ad stayed salted         | exact                        |

Flink JobManager after all runs: 1,475 Mi of 2 Gi, 0 restarts (r1 at 1 Gi: OOMKilled after 41 min).

## NFRs

| NFR                                   | Held? | Evidence / why                                                                                                                                                                                         |
| ------------------------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| High availability                     | Yes   | zone loss 100 % clicks OK; all Redis down → 0 errors; broker/DB/Flink kills ≤ 0.01 % errors                                                                                                            |
| No clicks lost on failure             | Yes\* | lost = 0 in every load and chaos run. \*A 503 after an in-flight Kafka timeout may still be counted (over-count ≤ ambiguous, e.g. 18/18 in #8). A **Docker Desktop hard stop** corrupted Flink's HA checkpoint in Floci (see below) |
| Click + redirect < 100 ms             | Mostly | p95 17–90 ms up to 2,000 rps; 10 s tail spikes to ~300 ms during the 2,000 rps hold                                                                                                                  |
| Metrics query < 500 ms                | Yes   | p95 11–16 ms                                                                                                                                                                                           |
| Eventual consistency ≤ 1 min          | Yes, except during Flink recovery | 0.5–2 s normally; JM kill ≈ 65 s, analytics-db partition 106 s, rescale gap ~65 s                                                                                                     |
| Scale (lab: 500 rps, 1,000 burst, 2,000 ramp) | Yes   | 2,000 rps with 15 receivers; Flink stayed at 3 (lag never exceeded ~500 records, so the autoscaler didn't need to act)                                                                                  |

## Known issues / findings

1. **Hard stop of Docker Desktop** (quit/reboot with the lab running) is the one failure that
   wasn't survived cleanly: Redis AOF tails got corrupted (fixed: `aof-load-corrupt-tail-max-size`)
   and Floci kept a 0-byte Flink HA checkpoint object, leaving the job stuck in `INITIALIZING`.
   Recovery: `pipelines/click-aggregator/reset.sh` then redeploy (loses open-minute state). Prefer
   `make down`, or stop load, before quitting Docker.
2. **`make ci` stalls on the Flink pipeline** after its FlinkDeployment was deleted: the image builds
   in seconds but Tilt never applies the FlinkDeployment (the manifest applies fine by hand). Worked
   around by applying it directly; needs a look at the pipeline Tiltfile / `k8s_kind` setup.
3. **Tail latency at 2,000 rps** (10 s p95 up to ~300 ms) — next suspects: Redis dedup or Kafka
   produce latency under peak, HPA adding pods, GC.
4. **Redis silent primary death** (the receivers' slot-map refresh) is unit-tested but wasn't
   re-exercised on the cluster in round 2: zone-b only held replicas when it was stopped.
5. **Sandbox tooling:** from Claude Code, bare `kubectl` and `curl localhost:8080` ran sandboxed
   despite `kubectl *` in `excludedCommands`; `chaos/node-down.sh` isn't excluded. Use
   `docker exec sdl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf …` meanwhile.
6. `chaos/redis-primary-kill.yaml` names a pod: re-check `redis-cli cluster nodes` before running it.

## Next experiments

- **Client-supplied click ids + dedup on `click_id` in Flink** — removes the ambiguous over-count
  (exactly-once end to end, not just "lost = 0").
- **Batch reconciliation path** (raw clicks → S3 → periodic recount) for late events > 1 h and for
  the hard-stop case.
- Push past 2,000 rps to make the **Flink autoscaler** act, and find the analytics-db write ceiling
  (~1,200 row updates/s seen).
- Kill a Redis **primary's node** (silent death) to exercise the slot-map refresh on the cluster.
- Synchronous replication on analytics-db: cost vs. no lost aggregates on failover.
- JWT key rotation (two `kid`s in the JWKS) without downtime.
