#!/usr/bin/env bash
# Removes the Kafka cluster, its topics and data (the Strimzi operator stays).
source "$(dirname "$0")/../../../scripts/lib.sh"
kc -n apps delete secret kafka-conn --ignore-not-found >/dev/null
kc -n data delete kafkatopics.kafka.strimzi.io -l strimzi.io/cluster=kafka --ignore-not-found >/dev/null
kc -n data delete kafka.kafka.strimzi.io/kafka --ignore-not-found --wait >/dev/null
kc -n data delete kafkanodepool.kafka.strimzi.io/dual-role podmonitor/kafka configmap/kafka-metrics --ignore-not-found >/dev/null
ok "kafka removed"
