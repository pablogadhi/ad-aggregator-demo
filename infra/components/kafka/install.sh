#!/usr/bin/env bash
# usage: install.sh [small|ha]   — Strimzi operator + Kafka cluster "kafka" (KRaft) + apps/kafka-conn
# (a second arg — the stack.yaml instance — is accepted but must be "kafka": one cluster per design)
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-kafka}
STRIMZI_CHART_VERSION=1.2.0   # operator 1.2.0; Kafka 4.3.1 (image quay.io/strimzi/kafka:1.2.0-kafka-4.3.1)

[ -d "$HERE/profiles/$PROFILE" ] || die "kafka: unknown profile '$PROFILE'"
[ "$INSTANCE" = kafka ] || die "kafka: only one instance (named 'kafka') is supported, got '$INSTANCE'"

helm_repo strimzi https://strimzi.io/charts/
helm repo update strimzi >/dev/null
helm_install strimzi strimzi/strimzi-kafka-operator "$STRIMZI_CHART_VERSION" strimzi-system -f "$HERE/values/operator.yaml"

log "kafka: applying profile '$PROFILE'"
kc apply -k "$HERE/profiles/$PROFILE" >/dev/null
wait_for "kafka cluster to be created" 60 kc -n data get kafka.kafka.strimzi.io/kafka
kc -n data wait --for=condition=Ready kafka.kafka.strimzi.io/kafka --timeout=900s >/dev/null
wait_for "kafka exporter" 300 kc -n data rollout status deploy/kafka-kafka-exporter --timeout=10s

# Connection contract: Secret kafka-conn in namespace apps (plain listener, no auth inside the cluster)
kc -n apps create secret generic kafka-conn \
  --from-literal=BOOTSTRAP_SERVERS=kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
  --dry-run=client -o yaml | kc label --local -f - sdl.dev/conn=true -o yaml | kc apply -f - >/dev/null

ok "kafka ($PROFILE) ready — secret apps/kafka-conn"
