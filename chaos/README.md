# Chaos experiments

Failure injection for the running design. Chaos Mesh (dashboard: <http://chaos.localhost:8080>) handles
pod/network/IO/time faults; `node-down.sh` stops whole kind nodes (servers / zones).

```bash
make chaos E=<file-without-.yaml>     # apply one experiment
make chaos-clear                      # remove all of them
chaos/node-down.sh zone-a             # stop every node in zone-a; --restore to bring them back
```

Run experiments **under load** so you can see the effect: `make load &` then apply the experiment ~15s in.

| Experiment              | What it does                                             | Verified behaviour on the sample stack                                                                                                                          |
| ----------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postgres-primary-kill` | kills the CNPG primary                                   | failover in ~20s to another zone; ~3.5% of writes failed, write p95 rose to 2s (max = 15s route timeout); replica reads unaffected; `make e2e` green afterwards |
| `zone-a-latency`        | +100ms between zone-a app pods and the data tier (5 min) | zone-a pod ~320ms/request (several DB round trips), other zones unaffected                                                                                      |
| `partition-apps-data`   | zone-b app pods cannot reach the data tier (2 min)       | zone-b pod fails `/readyz` → gateway stops routing to it, 100% of requests still succeed via zones a/c                                                          |

## ad-aggregator experiments (design/spec.md §11)

Every run: start the load, inject the fault ~60 s in, then reconcile and re-run the acceptance flows:

```bash
k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=chaos -e RUN=<exp> --out csv=loadtest/results/<exp>.csv loadtest/ad-aggregator.js
make chaos E=<exp>                                      # ~60 s into the run (or the command in the table)
k6 run -e BASE_URL=http://localhost:8080 -e RUN=<exp> loadtest/reconcile.js     # k6 accepted == analytics totals
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
