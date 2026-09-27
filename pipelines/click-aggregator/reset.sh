#!/usr/bin/env bash
# usage: pipelines/click-aggregator/reset.sh
#
# Recovery for a job stuck in INITIALIZING because its HA/checkpoint state in S3 is unreadable —
# seen after Docker Desktop was quit / the host restarted with the lab running (Floci can leave a
# 0-byte object whose metadata declares a size, and the JobManager retries restoring it forever).
# Pod-level chaos (TM/JM kill, node down) does NOT need this; checkpoint recovery handles those.
#
# Deleting the FlinkDeployment makes the operator delete the job's Kubernetes HA ConfigMaps (the
# pointers to the bad checkpoint). Redeploying then starts from the Kafka offsets last committed by
# the consumer group `click-aggregator`. Aggregation state for minutes still open at the time of the
# crash is lost (their rows may be overwritten with partial counts) — acceptable only after a
# hard stop; say so in any results you report.
source "$(dirname "$0")/../../scripts/lib.sh"

no_pods() { [ -z "$(kc -n apps get pods -l app=click-aggregator -o name)" ]; }

kc -n apps delete flinkdeployment click-aggregator --ignore-not-found --wait
wait_for "click-aggregator pods to terminate" 120 no_pods
ok "click-aggregator deleted (HA state cleared). Redeploy with 'make ci' (or 'make dev')."
