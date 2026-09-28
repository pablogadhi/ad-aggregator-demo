#!/usr/bin/env bash
# usage: install.sh [small|ha]   — Strimzi operator + kafka-glue (Kafka+KafkaNodePool, KRaft) + kafka-ui
# (a second arg — the stack.yaml instance — is accepted but must be "kafka": one cluster per design)
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-kafka}
KAFKA_UI=${KAFKA_UI:-true}   # set KAFKA_UI=false to skip the UI (e.g. constrained CI runs)

STRIMZI_CHART_VERSION=1.2.0   # operator 1.2.0; Kafka 4.3.1 (image quay.io/strimzi/kafka:1.2.0-kafka-4.3.1)
RAW_CHART_VERSION=2.0.2       # bedag/raw (glue: Kafka, KafkaNodePool, metrics, kafka-conn, kafka-ui route)
KAFKA_UI_CHART_VERSION=1.6.5  # kafbat/kafka-ui, app v1.5.0

[ -f "$HERE/values/$PROFILE.yaml" ] || die "kafka: unknown profile '$PROFILE'"
[ "$INSTANCE" = kafka ] || die "kafka: only one instance (named 'kafka') is supported, got '$INSTANCE'"

helm_repo strimzi https://strimzi.io/charts/
helm_repo bedag https://bedag.github.io/helm-charts
helm_repo kafbat https://ui.charts.kafbat.io/
helm repo update strimzi bedag kafbat >/dev/null

helm_install strimzi strimzi/strimzi-kafka-operator "$STRIMZI_CHART_VERSION" strimzi-system -f "$HERE/values/operator.yaml"

helm_install kafka-glue bedag/raw "$RAW_CHART_VERSION" data -f "$HERE/values/$PROFILE.yaml"
kc -n data wait --for=condition=Ready kafka.kafka.strimzi.io/kafka --timeout=900s >/dev/null
wait_for "kafka exporter" 300 kc -n data rollout status deploy/kafka-kafka-exporter --timeout=10s

if [ "$KAFKA_UI" = true ]; then
  helm_install kafka-ui kafbat/kafka-ui "$KAFKA_UI_CHART_VERSION" data -f "$HERE/values/kafka-ui.yaml"
fi

ok "kafka ($PROFILE) ready — secret apps/kafka-conn$([ "$KAFKA_UI" = true ] && echo ', kafka-ui.localhost:8080')"
