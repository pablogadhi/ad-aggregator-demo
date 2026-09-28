# Load tests (design/spec.md §10)

**Primary: in-cluster, via Grafana k6-operator** (namespace `k6`).

```bash
make load S=clicks [RUN=<id>]                       # SCENARIO=clicks|hot|scale|chaos, default clicks
RATE=.. DURATION=.. HOT=1 WRITES=1 QUERIES=1 EXPECT_SALTING=false TIMEOUT=90 AVOID_ZONE=zone-b \
  make load S=chaos                                  # env vars pass through to the k6 script
```

`loadtest/run.sh` (called by the `load` Makefile target, which stays thin):

1. Refreshes the `loadtest-scripts` ConfigMap in ns `k6` from `ad-aggregator.js` / `reconcile.js`.
2. Discovers the Envoy Gateway's in-cluster Service for Gateway `sdl` (`envoy-gateway-system`) and
   builds `BASE_URL` from it, so the run goes gateway -> services exactly like `localhost:8080`.
3. Applies a `TestRun` (`k6.io/v1alpha1`) for `ad-aggregator.js`, `parallelism: 3`, runners spread
   across zones via a `topologySpreadConstraints` on `topology.kubernetes.io/zone` (`AVOID_ZONE=<zone>`
   adds a `NotIn` node affinity too — chaos procedure, see `chaos/README.md`). k6 pushes results to
   Prometheus remote write (`--out experimental-prometheus-rw --tag testid=<RUN>`,
   `K6_PROMETHEUS_RW_SERVER_URL` = the kps Prometheus service `/api/v1/write`); `handleSummary` still
   prints a per-runner JSON summary to the runner's own logs.
4. Waits for the TestRun to finish, prints each runner's node/zone/exit code and last log lines, then
   runs `loadtest/gate.py <scenario> <run>` — **k6-operator evaluates thresholds per runner**, so this
   queries the *aggregated* Prometheus series for `testid=<RUN>` (percentiles: max of each runner's
   own percentile, since this lab's Prometheus isn't configured for k6 native histograms, which would
   allow a true merged `histogram_quantile`; rates: computed from additive Counters, exact) and applies
   the same thresholds as `ad-aggregator.js`'s `thresholds()`. This is the real gate; per-runner
   thresholds in the k6-operator logs are a sanity check only.
5. Applies a `parallelism: 1` `TestRun` for `reconcile.js`, waits, prints its log (lost / over-count /
   ambiguous / lag / PASS-FAIL) and folds its exit code into the overall result.

- `ad-aggregator.js`: `SCENARIO=clicks` (500 rps × 5 min + 1,000 rps × 1 min, + 20 rps queries), `hot`
  (20 % on one ad; `EXPECT_SALTING=false` when the receivers run with `HOT_SALTING_ENABLED=false`),
  `scale` (200 → 2,000 rps over 10 min, hold 5 min), `chaos` (300 rps × 5 min, `HOT=1`, `RATE`,
  `DURATION`). Setup creates 100 advertisers × 20 ads **per runner** (k6-operator runs `setup()`
  independently per pod; k6's execution-segment machinery divides arrival-rate scenarios so the
  *combined* rate across the 3 runners still matches the numbers above). Accepted clicks are counted
  in a Counter tagged `advertiser` (the real advertiser id) instead of a local result file, so
  `reconcile.js` — a separate TestRun, no shared filesystem — can read them back from Prometheus.
- `reconcile.js`: reads k6's accepted-click counts per advertiser from Prometheus
  (`sum by (advertiser) (k6_clicks_accepted_adv_total{testid="<RUN>"})`) and the ambiguous count
  (`k6_clicks_503_total` + `k6_clicks_other_total`), then polls the analytics totals of those
  advertisers until they match (or `TIMEOUT`, default 90 s); prints the lag, lost, over-count and
  ambiguous count. Pass = lost == 0 (analytics >= k6 accepted for every advertiser) and
  over-count <= ambiguous; fails (non-zero exit) otherwise.
- `timeline.py`: error % and p95 per 10 s from `--out csv=loadtest/results/<RUN>.csv` (host k6 fallback
  only — the in-cluster runners don't write a shared CSV; use Grafana / the Prometheus series for
  chaos-run timelines instead, e.g. `k6_http_req_duration_p95{testid="<RUN>",kind="click"}`).
- The hot scenario's teardown reads per-partition message rates of `clicks` from Prometheus
  (kafka-exporter `kafka_topic_partition_current_offset`) and gates `max/min < 2` with salting.

## Host k6 fallback

Useful if you need `--out csv` for `timeline.py`, or don't want to wait on TestRun scheduling. Needs
host `k6` (not installed by this repo — `make doctor` reports whether it's on PATH) and Prometheus
remote write reachable via the gateway:

```bash
k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=clicks -e RUN=<id> \
  -e K6_PROMETHEUS_RW_SERVER_URL=http://localhost:8080/api/v1/write \
  -e PROM_URL=http://localhost:8080 --out experimental-prometheus-rw --tag testid=<id> \
  loadtest/ad-aggregator.js
k6 run -e BASE_URL=http://localhost:8080 -e RUN=<id> \
  -e PROM_URL=http://localhost:8080 -e PROM_HOST=prometheus.localhost loadtest/reconcile.js
python3 loadtest/gate.py clicks <id>   # same aggregated gates (parallelism 1, so "aggregated" == it)
```

(`K6_PROMETHEUS_RW_SERVER_URL`'s `/api/v1/write` needs a `Host: prometheus.localhost` header, which
`--out experimental-prometheus-rw` doesn't send — reachable through the gateway with a local
`/etc/hosts` entry for `prometheus.localhost`, or point it at `kubectl port-forward svc/kps-prometheus
-n monitoring 9090:9090` and use `http://localhost:9090/...` instead.)
