#!/usr/bin/env bash
# Proves real behaviour, not just "pods Running":
#   1. the topic operator turns a KafkaTopic CR into a real topic
#   2. a client in namespace apps produces (acks=all) and consumes through kafka-conn
#   3. (ha) every partition is fully in sync, replicas sit on 3 brokers in 3 different zones (rack-aware)
#   4. the Kafka exporter publishes per-partition offsets for the topic (used by load tests)
source "$(dirname "$0")/../../../scripts/lib.sh"

bs=$(kc -n apps get secret kafka-conn -o jsonpath='{.data.BOOTSTRAP_SERVERS}' | base64 -d)
[ -n "$bs" ] || die "kafka-conn: missing BOOTSTRAP_SERVERS"
brokers=$(kc -n data get kafkanodepool.kafka.strimzi.io/dual-role -o jsonpath='{.spec.replicas}')
image=$(kc -n data get pod -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].spec.containers[0].image}')
rf=$(( brokers >= 3 ? 3 : 1 )); isr=$(( brokers >= 3 ? 2 : 1 ))

log "kafka: KafkaTopic sdl-smoke (3 partitions, RF $rf)"
kc apply -f - >/dev/null <<YAML
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: sdl-smoke
  namespace: data
  labels: { strimzi.io/cluster: kafka }
spec:
  partitions: 3
  replicas: $rf
  config: { min.insync.replicas: $isr, retention.ms: 3600000 }
YAML
kc -n data wait --for=condition=Ready kafkatopic.kafka.strimzi.io/sdl-smoke --timeout=120s >/dev/null || die "KafkaTopic sdl-smoke not Ready"
ok "topic operator created sdl-smoke"

log "kafka: produce (acks=all) + consume from namespace apps"
token="smoke-$(date +%s)"
script="set -e; cd /opt/kafka
for i in \$(seq 1 30); do echo \"k\$i:$token-\$i\"; done \
  | bin/kafka-console-producer.sh --bootstrap-server $bs --topic sdl-smoke \
      --command-property acks=all --reader-property parse.key=true --reader-property key.separator=: 2>&1 | grep -v WARN || true
echo consumed=\$(bin/kafka-console-consumer.sh --bootstrap-server $bs --topic sdl-smoke --from-beginning \
      --group sdl-smoke-$token --timeout-ms 15000 2>/dev/null | grep -c '$token')"
out=$(run_once apps "$image" sh -c "$script") || die "kafka produce/consume failed: $out"
echo "$out" | grep -q "consumed=30" || die "kafka: produced 30 messages, consumer saw: $out"
ok "30 messages produced (acks=all) and consumed via kafka-conn"

if [ "$brokers" -ge 3 ]; then
  log "kafka: replication + rack awareness"
  pod=$(kc -n data get pod -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')
  desc=$(kc -n data exec "$pod" -c kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic sdl-smoke)
  echo "$desc" | awk '/Partition:/' | while read -r line; do
    reps=$(echo "$line" | sed -E 's/.*Replicas: ([0-9,]+).*/\1/'); in_sync=$(echo "$line" | sed -E 's/.*Isr: ([0-9,]+).*/\1/')
    [ "$(echo "$reps" | tr ',' '\n' | sort -u | wc -l)" -eq 3 ] || die "kafka: partition not on 3 brokers: $line"
    [ "$(echo "$in_sync" | tr ',' '\n' | wc -l)" -eq 3 ] || die "kafka: partition not fully in sync: $line"
  done
  ok "every partition has 3 replicas, all in sync"
  zones=$(for n in $(kc -n data get pod -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true -o jsonpath='{.items[*].spec.nodeName}'); do
    kc get node "$n" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}'; done | sort -u | wc -l)
  [ "$zones" -eq 3 ] || die "kafka: brokers span $zones zones, expected 3"
  racks=$(for p in $(kc -n data get pod -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true -o jsonpath='{.items[*].metadata.name}'); do
    id=${p##*-}   # pod kafka-dual-role-<node id>
    kc -n data exec "$pod" -c kafka -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type brokers --entity-name "$id" --all 2>/dev/null | grep -o '^ *broker.rack=[A-Za-z0-9._-]*' | tr -d ' ' | head -1; done | sort -u | wc -l)
  [ "$racks" -eq 3 ] || die "kafka: expected 3 distinct broker.rack values, got $racks"
  ok "brokers in 3 zones, broker.rack set per zone"
fi

log "kafka: exporter per-partition metrics"
exporter=$(kc -n data get pod -l strimzi.io/name=kafka-kafka-exporter -o jsonpath='{.items[0].metadata.name}')
for _ in 1 2 3 4 5 6 7 8 9 10; do
  n=$(kc get --raw "/api/v1/namespaces/data/pods/$exporter:9404/proxy/metrics" 2>/dev/null | grep -c '^kafka_topic_partition_current_offset{.*topic="sdl-smoke"' || true)
  [ "$n" -eq 3 ] && break; sleep 5
done
[ "$n" -eq 3 ] || die "kafka exporter: expected 3 partition offset series for sdl-smoke, got $n"
ok "exporter publishes kafka_topic_partition_current_offset per partition"
ok "kafka smoke passed"
