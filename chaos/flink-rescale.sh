#!/usr/bin/env bash
# Chaos #8: change the click-aggregator parallelism under load (default 3 -> 6) and back.
#   chaos/flink-rescale.sh 6      # during a SCENARIO=chaos load run
#   chaos/flink-rescale.sh 3      # restore
# Hypothesis: a rescale (adaptive scheduler in place, or last-state redeploy from the latest checkpoint) loses
# no counts. Verify: reconcile exact; job back to RUNNING; freshness gap = longest pause in click_counts.updated_at.
source "$(dirname "$0")/../scripts/lib.sh"
p=${1:-6}
kc -n apps patch flinkdeployment click-aggregator --type merge -p "{\"spec\":{\"job\":{\"parallelism\":$p}}}"
ok "parallelism -> $p; watch: kubectl -n apps get flinkdeployment click-aggregator -w"
