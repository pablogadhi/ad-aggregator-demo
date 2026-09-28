#!/usr/bin/env bash
# Removes kafka-ui, the Kafka cluster/topics/data (kafka-glue) — the Strimzi operator stays.
source "$(dirname "$0")/../../../scripts/lib.sh"
helm uninstall kafka-ui --kube-context "$SDL_CLUSTER" -n data --ignore-not-found >/dev/null 2>&1 || true
kc -n data delete kafkatopics.kafka.strimzi.io -l strimzi.io/cluster=kafka --ignore-not-found >/dev/null
helm uninstall kafka-glue --kube-context "$SDL_CLUSTER" -n data --ignore-not-found >/dev/null
ok "kafka removed"
