#!/usr/bin/env bash
# Proves real behaviour, not just "operator Running":
#   1. the operator (watching ns apps) turns a FlinkDeployment into a RUNNING job (Flink 2.2.1)
#   2. the job's REST API answers through flink-conn's REST_URL service (flink-rest)
#   3. if the aws component is installed: checkpoints complete on S3 (Floci) via the built-in
#      flink-s3-fs-presto plugin with path-style access — the setup pipelines use for state + HA
#   then deletes the smoke job.
source "$(dirname "$0")/../../../scripts/lib.sh"
FLINK_IMAGE=flink:2.2.1-scala_2.12-java17
AWS_CLI_IMAGE=amazon/aws-cli:2.37.4

rest=$(kc -n apps get secret flink-conn -o jsonpath='{.data.REST_URL}' | base64 -d)
[ -n "$rest" ] || die "flink-conn: missing REST_URL"
kc -n flink-operator rollout status deploy/flink-kubernetes-operator --timeout=120s >/dev/null || die "flink operator not ready"

s3conf=""; s3env=""
if kc -n apps get secret aws-conn >/dev/null 2>&1; then
  aget() { kc -n apps get secret aws-conn -o jsonpath="{.data.$1}" | base64 -d; }
  endpoint=$(aget ENDPOINT_URL); akid=$(aget ACCESS_KEY_ID); secret=$(aget SECRET_ACCESS_KEY)
  run_once apps "$AWS_CLI_IMAGE" sh -c "AWS_ENDPOINT_URL=$endpoint AWS_REGION=$(aget REGION) AWS_ACCESS_KEY_ID=$akid AWS_SECRET_ACCESS_KEY=$secret \
    aws s3api create-bucket --bucket sdl-flink-smoke >/dev/null 2>&1 || true" >/dev/null || true
  s3conf="
    execution.checkpointing.interval: 5s
    execution.checkpointing.dir: s3://sdl-flink-smoke/checkpoints
    s3.endpoint: $endpoint
    s3.path.style.access: \"true\"
    s3.access-key: $akid
    s3.secret-key: $secret"
  s3env="
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - { name: ENABLE_BUILT_IN_PLUGINS, value: flink-s3-fs-presto-2.2.1.jar }"
fi

log "flink: FlinkDeployment sdl-smoke (StateMachineExample) in ns apps"
manifest="apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: sdl-smoke
  namespace: apps
spec:
  image: $FLINK_IMAGE
  flinkVersion: v2_2
  serviceAccount: flink
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: \"1\"$s3conf
  jobManager:
    resource: { memory: 1024m, cpu: 0.5 }
  taskManager:
    resource: { memory: 1024m, cpu: 0.5 }$s3env
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 1
    upgradeMode: stateless"
echo "$manifest" | kc apply -f - >/dev/null
cleanup() { kc -n apps delete flinkdeployment sdl-smoke --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

state=""
for _ in $(seq 1 100); do
  state=$(kc -n apps get flinkdeployment sdl-smoke -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)
  [ "$state" = RUNNING ] && break
  sleep 3
done
if [ "$state" != RUNNING ]; then
  kc -n apps get flinkdeployment sdl-smoke -o jsonpath='{.status.error}' >&2 || true
  die "flink: job state '$state', expected RUNNING"
fi
ok "FlinkDeployment sdl-smoke RUNNING"

svc=${rest#http://}; svc=${svc%%.*}
jobs=""
for _ in $(seq 1 20); do
  jobs=$(kc get --raw "/api/v1/namespaces/apps/services/$svc:8081/proxy/jobs/overview" 2>/dev/null || true)
  echo "$jobs" | grep -q '"state":"RUNNING"' && break
  sleep 3
done
echo "$jobs" | grep -q '"state":"RUNNING"' || die "flink: REST $rest/jobs/overview does not show a RUNNING job: $jobs"
jid=$(echo "$jobs" | python3 -c 'import json,sys; print(json.load(sys.stdin)["jobs"][0]["jid"])')
ok "REST_URL ($rest) answers: job $jid RUNNING"

if [ -n "$s3conf" ]; then
  log "flink: checkpoints to S3 (Floci)"
  completed=0
  for _ in $(seq 1 30); do
    completed=$(kc get --raw "/api/v1/namespaces/apps/services/$svc:8081/proxy/jobs/$jid/checkpoints" 2>/dev/null \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["counts"]["completed"])' 2>/dev/null || echo 0)
    [ "$completed" -ge 2 ] && break
    sleep 3
  done
  [ "$completed" -ge 2 ] || die "flink: no completed checkpoints on s3://sdl-flink-smoke (got $completed)"
  ok "$completed checkpoints completed on s3://sdl-flink-smoke via flink-s3-fs-presto"
fi

cleanup
trap - EXIT
wait_for "sdl-smoke job pods to go away" 120 sh -c "[ -z \"\$(kubectl --context $SDL_CLUSTER -n apps get pods -l app=sdl-smoke -o name)\" ]"
ok "flink smoke passed (smoke job deleted)"
