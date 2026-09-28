#!/usr/bin/env bash
# In-cluster k6 load test (design/spec.md §10; loadtest/README.md), run by the `make load` target.
# k6-operator (namespace k6) runs loadtest/ad-aggregator.js as a TestRun with parallelism 3, runners
# spread across zones, hitting the gateway's in-cluster Service; results go to Prometheus remote
# write tagged `testid=<RUN>`. This wrapper gates on the *aggregated* Prometheus series (k6-operator
# only evaluates thresholds per runner) via loadtest/gate.py, then runs reconcile.js as its own
# TestRun (parallelism 1), which reads k6's per-advertiser accepted counts back from Prometheus.
#
#   make load S=clicks [RUN=<id>]
#   RATE=.. DURATION=.. HOT=1 WRITES=1 QUERIES=1 EXPECT_SALTING=false TIMEOUT=90 AVOID_ZONE=zone-b \
#     make load S=chaos E=<exp>   # chaos procedure (chaos/README.md): AVOID_ZONE keeps runners off
#                                  # the zone/node a chaos experiment is about to take down
#
# Host k6 fallback: see the header comments in loadtest/ad-aggregator.js and loadtest/reconcile.js.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=../scripts/lib.sh
source scripts/lib.sh

SCENARIO="${1:-clicks}"
RUN="${RUN:-${SCENARIO}-$(date +%s)}"
# k8s object names: lowercase alnum + '-'
RUN="$(printf '%s' "$RUN" | tr '[:upper:]_.' '[:lower:]--' | tr -cd 'a-z0-9-')"
NAME="k6-${RUN}"
NAME_RC="k6-${RUN}-rc"
TIMEOUT="${TIMEOUT:-90}"
MAX_WAIT="${MAX_WAIT:-1200}" # 20 min: covers clicks/hot/chaos and the 15 min `scale` scenario

ensure_ns k6
log "load: scenario=$SCENARIO run=$RUN (TestRun $NAME, reconcile $NAME_RC)"

# --- ConfigMap, refreshed every run -----------------------------------------------------------
kc -n k6 create configmap loadtest-scripts \
  --from-file=ad-aggregator.js=loadtest/ad-aggregator.js \
  --from-file=reconcile.js=loadtest/reconcile.js \
  --dry-run=client -o yaml | kc apply -f - >/dev/null

# --- targets: the gateway's in-cluster Service for Gateway `sdl`, and Prometheus's -------------
GW_SVC=$(kc -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=sdl -o jsonpath='{.items[0].metadata.name}')
[ -n "$GW_SVC" ] || die "envoy gateway Service for Gateway sdl not found in envoy-gateway-system"
BASE_URL="http://${GW_SVC}.envoy-gateway-system.svc.cluster.local"
PROM_INCLUSTER="http://kps-prometheus.monitoring.svc.cluster.local:9090"
TREND_STATS="min,p(90),p(95),p(99),max"

# --- runner zone-avoidance (AVOID_ZONE=zone-x): keep runner pods off a zone under chaos ---------
affinity_yaml() {
  [ -z "${AVOID_ZONE:-}" ] && return 0
  cat <<YAML
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: topology.kubernetes.io/zone
                  operator: NotIn
                  values: ["${AVOID_ZONE}"]
YAML
}

# --- wait for a TestRun to reach stage 'finished'/'stopped'/'error' -----------------------------
wait_testrun() {
  local name=$1 start; start=$(date +%s)
  local stage=""
  while true; do
    stage=$(kc -n k6 get testrun "$name" -o jsonpath='{.status.stage}' 2>/dev/null || true)
    case "$stage" in
      finished|stopped|error) echo "$stage"; return 0 ;;
    esac
    if (( $(date +%s) - start > MAX_WAIT )); then echo "timeout"; return 1; fi
    sleep 5
  done
}

# --- pods' nodes/zones + exit codes for a TestRun's runner pods --------------------------------
report_runners() {
  local name=$1
  kc -n k6 get pods -l "k6_cr=${name},runner=true" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.nodeName}{"\t"}{.status.containerStatuses[0].state.terminated.exitCode}{"\n"}{end}' |
  while IFS=$'\t' read -r pod node code; do
    zone=$(kc get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
    echo "  runner $pod  node=$node zone=$zone exit=$code"
  done
}

runner_exit_codes() {
  local name=$1
  kc -n k6 get pods -l "k6_cr=${name},runner=true" \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].state.terminated.exitCode}{"\n"}{end}'
}

# =================================================================================================
# 1) load TestRun (parallelism 3, spread across zones)
# =================================================================================================
kc apply -f - <<YAML >/dev/null
apiVersion: k6.io/v1alpha1
kind: TestRun
metadata:
  name: ${NAME}
  namespace: k6
spec:
  parallelism: 3
  script:
    configMap:
      name: loadtest-scripts
      file: ad-aggregator.js
  arguments: "--out experimental-prometheus-rw --tag testid=${RUN}"
  runner:
    env:
      - {name: BASE_URL, value: "${BASE_URL}"}
      - {name: SCENARIO, value: "${SCENARIO}"}
      - {name: RUN, value: "${RUN}"}
      - {name: RATE, value: "${RATE:-}"}
      - {name: DURATION, value: "${DURATION:-}"}
      - {name: HOT, value: "${HOT:-}"}
      - {name: WRITES, value: "${WRITES:-}"}
      - {name: QUERIES, value: "${QUERIES:-}"}
      - {name: EXPECT_SALTING, value: "${EXPECT_SALTING:-}"}
      - {name: PROM_URL, value: "${PROM_INCLUSTER}"}
      - {name: K6_PROMETHEUS_RW_SERVER_URL, value: "${PROM_INCLUSTER}/api/v1/write"}
      - {name: K6_PROMETHEUS_RW_TREND_STATS, value: "${TREND_STATS}"}
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels: {k6_cr: "${NAME}", runner: "true"}
$(affinity_yaml)
YAML

log "load TestRun $NAME applied; waiting (max ${MAX_WAIT}s)..."
stage=$(wait_testrun "$NAME")
log "load TestRun $NAME: stage=$stage"
report_runners "$NAME"

log "--- runner logs (last 20 lines each) ---"
for pod in $(kc -n k6 get pods -l "k6_cr=${NAME},runner=true" -o jsonpath='{.items[*].metadata.name}'); do
  echo "-- $pod --"
  kc -n k6 logs "$pod" --tail=20 || true
done

# =================================================================================================
# 2) aggregated Prometheus gates (thresholds are per-runner in k6-operator; this is the real gate)
# =================================================================================================
gates_rc=0
python3 loadtest/gate.py "$SCENARIO" "$RUN" "http://localhost:8080" "prometheus.localhost" || gates_rc=$?

# =================================================================================================
# 3) reconcile TestRun (parallelism 1): k6 accepted (from Prometheus) vs analytics
# =================================================================================================
kc apply -f - <<YAML >/dev/null
apiVersion: k6.io/v1alpha1
kind: TestRun
metadata:
  name: ${NAME_RC}
  namespace: k6
spec:
  parallelism: 1
  script:
    configMap:
      name: loadtest-scripts
      file: reconcile.js
  runner:
    env:
      - {name: BASE_URL, value: "${BASE_URL}"}
      - {name: RUN, value: "${RUN}"}
      - {name: TIMEOUT, value: "${TIMEOUT}"}
      - {name: PROM_URL, value: "${PROM_INCLUSTER}"}
YAML

log "reconcile TestRun $NAME_RC applied; waiting..."
stage=$(wait_testrun "$NAME_RC")
log "reconcile TestRun $NAME_RC: stage=$stage"
report_runners "$NAME_RC"

log "--- reconcile logs ---"
rc_pod=$(kc -n k6 get pods -l "k6_cr=${NAME_RC},runner=true" -o jsonpath='{.items[0].metadata.name}')
kc -n k6 logs "$rc_pod" || true

reconcile_rc=0
for code in $(runner_exit_codes "$NAME_RC"); do
  [ "$code" = "0" ] || reconcile_rc=1
done

if [ "$gates_rc" -ne 0 ]; then warn "aggregated Prometheus gates FAILED"; fi
if [ "$reconcile_rc" -ne 0 ]; then warn "reconcile FAILED"; fi
[ "$gates_rc" -eq 0 ] && [ "$reconcile_rc" -eq 0 ] && log "load $RUN: PASS" || die "load $RUN: FAIL (gates=$gates_rc reconcile=$reconcile_rc)"
