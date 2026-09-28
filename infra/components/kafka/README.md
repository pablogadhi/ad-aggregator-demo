# kafka (Strimzi, KRaft)

Apache Kafka **4.3.1** managed by the Strimzi operator (**chart/operator 1.2.0**, API
`kafka.strimzi.io/v1`, image `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1`). KRaft only: one
`KafkaNodePool` (`dual-role`) whose nodes are both controllers and brokers. Operator in
`strimzi-system` (watches `data`), cluster `kafka` in `data`. No maintained chart deploys a Strimzi
cluster, so the `Kafka`/`KafkaNodePool` CRs, the JMX metrics ConfigMap, the PodMonitor, `kafka-conn`
and the `kafka-ui` HTTPRoute are **plain YAML** in the Flux base (nothing is generated, so no `bedag/raw`):

- `flux/operator/` → Kustomization `kafka-operator`: HelmRepositories `strimzi`, `kafbat`; HelmRelease
  `strimzi` (values `values/operator.yaml`). Its CRDs exist before the cluster CRs are applied.
- `flux/instance/` (shared): `glue.yaml` (metrics ConfigMap, PodMonitor, `apps/kafka-conn`, HTTPRoute) +
  HelmRelease `kafka-ui` (values `values/kafka-ui.yaml`, Helm drift detection on).
- `flux/small/`, `flux/ha/` → Kustomization `kafka` (stack.py picks the profile's directory): the shared
  instance + that profile's `kafka.yaml` (`Kafka` + `KafkaNodePool`). Ready only once Strimzi reports the
  `Kafka` Ready.

| Release (namespace)     | Chart                        | What                                                                 |
| ------------------------ | ----------------------------- | --------------------------------------------------------------------- |
| `strimzi` (`strimzi-system`) | `strimzi/strimzi-kafka-operator` 1.2.0 | operator, watches `data`                                     |
| `kafka-ui` (`data`)      | `kafbat/kafka-ui` 1.6.5 (app v1.5.0) | web UI, points at `kafka-kafka-bootstrap.data:9092` |

One instance per design, named `kafka` (`flux/single-instance`: `stack.py validate` rejects another name).

## Profiles

| Profile | Nodes                                   | Defaults                                   | Use                                   |
| ------- | --------------------------------------- | ------------------------------------------ | ------------------------------------- |
| `small` | 1 dual-role node (768Mi, 512m heap)     | RF 1, minISR 1                             | fastest, no failover                  |
| `ha`    | 3 dual-role nodes, **one per zone**     | RF 3, minISR 2, rack-aware (`broker.rack`) | broker / zone loss with `acks=all`    |

`ha` details: pods `kafka-dual-role-{0,1,2}` spread with a `DoNotSchedule` zone constraint;
`rack.topologyKey: topology.kubernetes.io/zone` sets `broker.rack` = zone, so the replicas of every
partition land in 3 different zones, and `RackAwareReplicaSelector` lets consumers that set
`client.rack` fetch from the replica in their zone. `auto.create.topics.enable=false`: topics are
`KafkaTopic` CRs in the design's `infra/design/` (the topic operator creates them).

Requests (ha): 3 × (200m, 1Gi; limit 1.5Gi, heap 768m) + topic operator 192Mi + exporter 64Mi.

## Connection contract — Secret `apps/kafka-conn`

| Key                 | Env (with `connections: [kafka]`) | Value                                              |
| ------------------- | --------------------------------- | -------------------------------------------------- |
| `BOOTSTRAP_SERVERS` | `KAFKA_BOOTSTRAP_SERVERS`         | `kafka-kafka-bootstrap.data.svc.cluster.local:9092` |

PLAINTEXT listener, no auth (no `SECURITY_PROTOCOL` key). Producers that must not lose data use
`acks=all` + `enable.idempotence=true` (with minISR 2 a write survives one broker loss).

## Topics (design-owned)

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata: { name: clicks, namespace: data, labels: { strimzi.io/cluster: kafka } }
spec: { partitions: 12, replicas: 3, config: { min.insync.replicas: 2, retention.ms: 86400000 } }
```

## Metrics

- **Kafka exporter** (`kafka-kafka-exporter`, all topics + groups): per-partition
  `kafka_topic_partition_current_offset{topic,partition}` (→ `rate()` = messages/s per partition),
  `kafka_consumergroup_lag{consumergroup,topic,partition}`, ISR/leader gauges.
- **Brokers**: JMX exporter with Strimzi's rules (`kafka_server_*`, `kafka_controller_*`,
  under-replicated partitions, request latencies).
- Both are scraped by PodMonitor `kafka` (port `tcp-prometheus`), e.g. per-partition rate of `clicks`:

  ```promql
  sum by (partition) (rate(kafka_topic_partition_current_offset{topic="clicks"}[1m]))
  ```

## kafka-ui

`kafka-ui.localhost:8080` (through the gateway, HTTPRoute in `flux/instance/glue.yaml`). Points at
`kafka-kafka-bootstrap.data.svc.cluster.local:9092` (`auth: disabled` — no login, lab only).
Drift demo: `kubectl -n data delete deploy kafka-ui` — helm-controller's drift detection recreates it on
the next HelmRelease reconcile (≤ 5 min, or `scripts/flux.sh reconcile helmrelease kafka-ui`).

## Operating it

```bash
kubectl -n data get kafka,kafkanodepool,kafkatopic
kubectl -n data exec -it kafka-dual-role-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic clicks
kubectl -n data exec -it kafka-dual-role-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group>
```

## Smoke test

`smoke.sh`: creates `KafkaTopic sdl-smoke` through the topic operator, produces 30 keyed messages
with `acks=all` and consumes them from namespace `apps` via `kafka-conn`; on `ha` checks that every
partition has 3 in-sync replicas on brokers in 3 zones with distinct `broker.rack`; checks the
exporter publishes a per-partition offset series for each partition; if `kafka-ui` is installed,
checks its API lists `sdl-smoke`.

## Experiments

- Kill the broker leading most partitions (PodChaos `pod-kill`, label
  `strimzi.io/pool-name=dual-role`) under load: with `acks=all`/minISR 2 producers see a short
  latency spike and retry, no acknowledged message is lost; leaders move within seconds.
- Zone loss (`chaos/node-down.sh` on a zone's only node): 2 of 3 replicas remain → still writable.
  Take down a second zone and minISR 2 makes `acks=all` producers fail (NotEnoughReplicas) instead of
  silently losing durability.
- Hot partition: produce with a skewed key and compare
  `rate(kafka_topic_partition_current_offset[1m])` per partition, then salt the key.
