# Chaos experiments

Failure injection for the running design. Chaos Mesh (dashboard: <http://chaos.localhost:8080>) handles
pod/network/IO/time faults; `node-down.sh` stops whole kind nodes (servers / zones).

```bash
make chaos E=<file-without-.yaml>     # apply one experiment
make chaos-clear                      # remove all of them
chaos/node-down.sh zone-a             # stop every node in zone-a; --restore to bring them back
```

Run experiments **under load** so you can see the effect: `make load S=chaos &` then apply the experiment
~60s in. Load runs in-cluster via k6-operator (`loadtest/README.md`); for an experiment that stops a
whole zone (#7, `node-down.sh`), pin the load's runners off that zone first with `AVOID_ZONE=<zone>` —
otherwise a runner pod can land on the node about to go down and the load stalls with it:

```bash
AVOID_ZONE=zone-b RATE=300 DURATION=5m make load S=chaos RUN=zone-down &
sleep 60 && chaos/node-down.sh sdl-worker2
```

| Experiment              | What it does                                             | Verified behaviour on the sample stack                                                                                                                          |
| ----------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postgres-primary-kill` | kills the CNPG primary                                   | failover in ~20s to another zone; ~3.5% of writes failed, write p95 rose to 2s (max = 15s route timeout); replica reads unaffected; `make e2e` green afterwards |
| `zone-a-latency`        | +100ms between zone-a app pods and the data tier (5 min) | zone-a pod ~320ms/request (several DB round trips), other zones unaffected                                                                                      |
| `partition-apps-data`   | zone-b app pods cannot reach the data tier (2 min)       | zone-b pod fails `/readyz` → gateway stops routing to it, 100% of requests still succeed via zones a/c                                                          |

## ad-aggregator experiments (design/spec.md §11)

Every run: start the load, inject the fault ~60 s in, then let `make load` finish (it reconciles on its
own), and re-run the acceptance flows. In-cluster (primary), `make load` runs `ad-aggregator.js` and
`reconcile.js` as TestRuns and prints the aggregated gates + reconcile PASS/FAIL itself:

```bash
make load S=chaos RUN=<exp> &                           # [AVOID_ZONE=<zone>] for a zone-down experiment
sleep 60 && make chaos E=<exp>                           # or the command in the table
wait                                                     # make load's own reconcile + gates
make chaos-clear && make e2e
```

For a per-10s error%/p95 timeline (`timeline.py`), use the host k6 fallback instead (it writes a local
CSV the in-cluster runners don't have a shared filesystem for):

```bash
k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=chaos -e RUN=<exp> \
  -e K6_PROMETHEUS_RW_SERVER_URL=http://localhost:8080/api/v1/write -e PROM_URL=http://localhost:8080 \
  --out experimental-prometheus-rw --tag testid=<exp> --out csv=loadtest/results/<exp>.csv loadtest/ad-aggregator.js
make chaos E=<exp>                                      # ~60 s into the run (or the command in the table)
k6 run -e BASE_URL=http://localhost:8080 -e RUN=<exp> -e PROM_URL=http://localhost:8080 -e PROM_HOST=prometheus.localhost loadtest/reconcile.js
python3 loadtest/timeline.py loadtest/results/<exp>.csv                        # error % + p95 per 10 s
make chaos-clear && make e2e
```

| # | Experiment (fault)                                        | Hypothesis                                                              | Verify                                                                                         |
| - | --------------------------------------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| 1 | `kafka-broker-kill` (set the pod to the top leader first) | no accepted click lost; errors < 1 %; p95 recovers < 30 s              | reconcile exact; timeline                                                                      |
| 2 | `flink-tm-kill`                                           | no counts lost; freshness recovers < 2 min                              | reconcile exact; `kubectl -n apps get flinkdeployment` RUNNING; consumer lag                   |
| 3 | `flink-jm-kill`                                           | HA + S3 checkpoints: no counts lost                                     | reconcile exact; new JM log "Restoring job … from Checkpoint"                                  |
| 4 | `redis-primary-kill` (set the pod to a current primary)   | clicks keep flowing (dedup fails open)                                  | errors < 1 %; `click_dedup_failopen_total` > 0; over-count reported by reconcile               |
| 5 | `postgres-primary-kill` (ads DB)                          | redirects for cached ads unaffected                                     | click success ≥ 99 %; ad-placement writes fail ≤ ~30 s                                         |
| 6 | `analytics-db-primary-kill`, `analytics-db-partition-flink` (60 s) | click path untouched; counts catch up with no loss             | zero click errors; reconcile exact; analytics queries recover < 60 s                           |
| 7 | `chaos/node-down.sh sdl-worker2` (all of zone-b) … `--restore` | clicks stay available                                              | click success ≥ 99 %; reconcile exact                                                          |
| 8 | `kubectl -n apps patch flinkdeployment click-aggregator --type merge -p '{"spec":{"job":{"parallelism":6}}}'` (`chaos/flink-rescale.sh 6`) | a rescale loses no counts | reconcile exact; job RUNNING; freshness gap |
| 9 | `redis-all-kill` under `-e HOT=1`                         | clicks flow, hot ad stays salted (last known set), markings resume      | errors < 1 %; `hot_ad_clicks_hot` ≈ 100 %; `/api/click-receiver/hot-ads` refreshes again        |

## Flux and chaos (self-healing infra)

Everything under `infra/` (platform, components, design infra) is reconciled by Flux: a manual change to a
Flux-managed object is reverted at the next reconcile (Kustomizations every 2 min for component instances
and `design`, 10 min for platform/operators; HelmReleases with drift detection — `kafka-ui`, `aws` — every
5 min). Pod-level faults (Chaos Mesh, `node-down.sh`) are unaffected: Flux manages the Deployments/CRs,
not their pods. The FlinkDeployment (#8) is Tilt-managed, so it isn't affected either.

**Before an experiment that edits a Flux-managed object** (scales a component, patches a `Kafka` CR or a
gateway policy, deletes a conn secret on purpose…), suspend its Kustomization, and resume it after:

```bash
scripts/flux.sh suspend kustomization kafka      # kafka, redis, postgres, analytics-db, aws, flink, design, platform…
# … experiment …
scripts/flux.sh resume kustomization kafka       # Flux re-applies the declared state
```

**Drift demo** (the self-healing is the experiment):

```bash
kubectl -n apps delete secret kafka-conn                   # a component's connection contract disappears
scripts/flux.sh get kustomization kafka                    # watch: recreated at the next reconcile (≤ 2 min)
scripts/flux.sh reconcile kustomization kafka --with-source   # or force it now
kubectl -n data delete deploy kafka-ui                     # helm-controller drift detection (≤ 5 min, or
scripts/flux.sh reconcile helmrelease kafka-ui             #   reconcile the HelmRelease) restores it
```

The web UI at <http://flux.localhost:8080> shows the tree platform → components → design and each
reconcile, and can suspend/resume/reconcile from the browser (anonymous lab admin).

## Writing new experiments (for designs)

Derive them from the spec's non-functional requirements. For each one write down the hypothesis,
e.g. _"no clicks lost when a Kafka broker dies"_ → kill a broker under load, then reconcile the counts.

Gotchas:

- **NetworkChaos + Services:** apps reach components through Service (ClusterIP) addresses, which kube-proxy
  translates _after_ the packet leaves the pod. With `direction: to`, Chaos Mesh filters on the target
  _pod_ IPs inside the source pod and matches nothing — the experiment reports "Injected" but has no
  effect. Use `direction: both` (the fault is also applied on the target side, where return traffic
  carries real pod IPs), or select the component pods as the source.
- Put experiments in namespace `chaos-mesh`; select targets with `selector.namespaces` + labels.
- `nodeSelectors: {topology.kubernetes.io/zone: zone-x}` scopes an experiment to one simulated AZ.
- `node-down.sh`: pods on a stopped node stay "Running" in the API for ~40s (until the node is NotReady)
  and are only evicted after the default 300s toleration. Stateful pods on local-path volumes cannot
  move — they come back with the node. This is real Kubernetes behaviour, worth observing.
