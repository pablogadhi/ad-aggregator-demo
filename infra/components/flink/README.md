# flink (Apache Flink Kubernetes Operator)

The **Apache Flink Kubernetes Operator 1.16.1** (helm chart 1.16.1 from
`archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/`, image
`ghcr.io/apache/flink-kubernetes-operator` at the tag pinned by that chart), in namespace
`flink-operator`, **watching namespace `apps`**. It installs no Flink cluster itself: pipelines
(`pipelines/<name>/`) deploy a `FlinkDeployment` in `apps`, so the job pods can read the `*-conn`
secrets. The chart creates the job service account **`flink`** (+ Role) in `apps`. The admission
webhook uses a cert-manager certificate (the platform installs cert-manager). The operator chart
provides no metrics PodMonitors, `flink-rest` Service or conn secret, so those are plain YAML in
`flux/instance/{monitoring,conn}.yaml` (Kustomization `flink`, after Kustomization `flink-operator` = `flux/operator/`:
HelmRepository + HelmRelease `flink-operator`, values `values/operator.yaml`).

One instance per design, named `flink`, one profile (`small`, the default; `flux/single-instance`, `flux/instance/`).

## Versions to build jobs against (verified by `smoke.sh`)

| What                  | Version / coordinates                                                                 |
| --------------------- | ------------------------------------------------------------------------------------- |
| Flink                 | **2.2.1** — image `flink:2.2.1-scala_2.12-java17`, `spec.flinkVersion: v2_2`           |
| Kafka connector       | `org.apache.flink:flink-connector-kafka:5.0.0-2.2` (DataStream/Table); SQL fat jar `flink-sql-connector-kafka-5.0.0-2.2.jar` |
| JDBC connector        | `org.apache.flink:flink-connector-jdbc-core:4.1.0-2.2` + `flink-connector-jdbc-postgres:4.1.0-2.2` (4.x split core + dialects; there is no monolithic `flink-connector-jdbc` for Flink 2.x) |
| Postgres JDBC driver  | `org.postgresql:postgresql:42.7.13`                                                    |
| PyFlink (if used)     | `apache-flink==2.2.1` (PyPI), on top of the `flink:2.2.1-…-java17` image               |
| S3 filesystem         | built-in plugin `flink-s3-fs-presto-2.2.1.jar` (`ENABLE_BUILT_IN_PLUGINS`); `flink-s3-fs-hadoop-2.2.1.jar` also ships in the image |

Why 2.2.1 and not 2.3.0: the operator supports up to `v2_4`, but the latest Kafka connector release
(5.0.0) is only built for Flink 2.1/2.2, and JDBC 4.1.0 likewise. Flink 2.2 is the newest version
with both connectors released.

Put connector jars in `/opt/flink/lib` of a custom image (`FROM flink:2.2.1-scala_2.12-java17`) —
or `/opt/flink/usrlib` for a DataStream fat jar.

## Profiles

| Profile | What                                                                 |
| ------- | -------------------------------------------------------------------- |
| `small` | operator (1 replica, 100m / 512Mi, limit 1Gi, heap 512m) + webhook (20m / 256Mi, limit 512Mi, heap 192m); jobs size themselves |

Budget a job at ~1 GiB per JM and per TM (process memory, `resource.memory: 1024m`); a TM with 1
slot is enough for simple SQL/DataStream stages. **Memory gotcha:** with a 1024m TM Flink's
defaults leave only ~26 MB task heap (managed memory takes 40 % ≈ 230 MB, JVM overhead/metaspace
the rest). With the default heap (`hashmap`) state backend set
`taskmanager.memory.managed.fraction: "0.05"` (task heap ≈ 200 MB); with RocksDB
(`state.backend.type: rocksdb`) keep the managed fraction — RocksDB lives in managed memory.

## Connection contract — Secret `apps/flink-conn`

| Key        | Env (with `connections: [flink]`) | Value                                                                        |
| ---------- | --------------------------------- | ---------------------------------------------------------------------------- |
| `REST_URL` | `FLINK_REST_URL`                  | `http://flink-rest.apps.svc.cluster.local:8081` — Service selecting the JobManager of any FlinkDeployment in `apps` |

With one FlinkDeployment per design this is "the job's REST API" (`/jobs/overview`,
`/jobs/<id>/checkpoints`, `/jobs/<id>/vertices/…` for parallelism). With several, use
`<deployment>-rest.apps.svc.cluster.local:8081`, which the operator creates per deployment.

## Writing a FlinkDeployment for this lab

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata: { name: my-job, namespace: apps }
spec:
  image: <registry>/my-job:tag            # FROM flink:2.2.1-scala_2.12-java17 + connector jars
  flinkVersion: v2_2
  serviceAccount: flink
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "1"
    # state + HA on S3 (aws component / Floci): values from aws-conn (the Tiltfile can template them)
    execution.checkpointing.interval: 10s
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.dir: s3://<bucket>/my-job/checkpoints
    execution.checkpointing.savepoint-dir: s3://<bucket>/my-job/savepoints
    high-availability.type: kubernetes
    high-availability.storageDir: s3://<bucket>/my-job/ha
    s3.endpoint: http://aws.data.svc.cluster.local:4566
    s3.path.style.access: "true"
    s3.access-key: test
    s3.secret-key: test
    # job autoscaler (built into the operator) + in-place rescaling
    job.autoscaler.enabled: "true"
    job.autoscaler.vertex.min-parallelism: "3"
    job.autoscaler.vertex.max-parallelism: "12"
    job.autoscaler.target.utilization: "0.7"
    job.autoscaler.stabilization.interval: 1m
    job.autoscaler.metrics.window: 3m
    jobmanager.scheduler: adaptive          # parallelism-only changes rescale in place
    pipeline.max-parallelism: "120"
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - { name: ENABLE_BUILT_IN_PLUGINS, value: flink-s3-fs-presto-2.2.1.jar }
          envFrom:
            - { secretRef: { name: kafka-conn }, prefix: KAFKA_ }
  jobManager: { resource: { memory: 1024m, cpu: 0.5 } }
  taskManager: { resource: { memory: 1024m, cpu: 0.5 } }
  job:
    jarURI: local:///opt/flink/usrlib/my-job.jar
    parallelism: 3
    upgradeMode: last-state                  # restore from the latest checkpoint on upgrades
```

Flink 2.x renamed the state keys: `execution.checkpointing.dir` (was `state.checkpoints.dir`),
`execution.checkpointing.savepoint-dir` (was `state.savepoints.dir`), `state.backend.type`.

## Metrics

- Operator defaults applied to every job: Prometheus reporter on port **9249**
  (`metrics.reporter.prom.*`). PodMonitor `apps/flink-jobs` scrapes JM + TMs of every
  FlinkDeployment (labels `flink_deployment`, `flink_component`): busy time, backpressure, Kafka
  source `pendingRecords`, checkpoints — what the autoscaler decides on.
- Operator metrics (`flink_k8soperator_*`, incl. autoscaler) on 9999, PodMonitor
  `flink-operator/flink-operator`.
- Parallelism over time: `kubectl -n apps get flinkdeployment <name> -o yaml`
  (`status.jobStatus`, autoscaler overrides in `spec.flinkConfiguration` /
  `pipeline.jobvertex-parallelism-overrides`), or `$FLINK_REST_URL/jobs/<id>`.

## Smoke test

`smoke.sh` deploys FlinkDeployment `sdl-smoke` (StateMachineExample, Flink 2.2.1, 1 JM + 1 TM) in
`apps`, waits for `RUNNING`, checks the job through `REST_URL`'s service, and — when the aws
component is installed — checkpoints every 5 s to `s3://sdl-flink-smoke` on Floci via
`flink-s3-fs-presto` and requires ≥ 2 completed checkpoints. Then it deletes the job.

## Experiments

- Kill a TaskManager: the job restarts from the last checkpoint (restart strategy), no state lost.
- Kill the JobManager: with Kubernetes HA + S3 storageDir the new JM recovers the job from the
  latest checkpoint (visible in the JM log: "Restoring job … from Checkpoint …").
- Change parallelism (or let the autoscaler do it) under load: with `jobmanager.scheduler:
  adaptive` the operator rescales in place; otherwise it redeploys from the latest checkpoint.
