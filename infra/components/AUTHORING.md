# Authoring a component

A _component_ is a piece of backing infrastructure (Kafka, Redis, Flink, …) that designs select in
`stack.yaml`. Components are built **on demand** the first time a design needs one, then harvested
back into the template (`scripts/harvest-component.sh`) so the next design reuses them.

`postgres/` is the reference implementation — copy its shape.

## Layout

A component is a **Flux base**: plain Flux objects that `make up` / `make sync` push (as an OCI artifact of
`infra/`) and Flux reconciles. No install scripts.

```
infra/components/<name>/
├── flux/
│   ├── operator/            # (if any) once per design → Kustomization <name>-operator
│   │   ├── kustomization.yaml   #   resources + configMapGenerator from ../../values/operator.yaml
│   │   ├── sources.yaml         #   HelmRepository / OCIRepository (charts the instances use too)
│   │   └── releases.yaml        #   the operator HelmRelease
│   ├── instance/            # once per stack.yaml entry → Kustomization <instance>
│   │   ├── kustomization.yaml   #   configMapGenerator ${INSTANCE}-values from ../../values/*.yaml
│   │   ├── releases.yaml        #   HelmRelease(s), named/configured with ${INSTANCE} and ${PROFILE}
│   │   └── conn.yaml, …         #   plain YAML named by content: conn.yaml (static conn secret),
│   │                            #   monitoring.yaml (PodMonitors), route.yaml (HTTPRoutes), CRs
│   ├── <profile>/           # (optional) overlay used instead of instance/ when profiles differ in plain
│   │                        #   YAML (kafka: Kafka + KafkaNodePool per profile on top of ../instance)
│   └── single-instance      # (optional) marker: stack.py validate rejects instance != <name>
├── smoke.sh                 # usage: smoke.sh [instance] — proves real behaviour; exit non-zero on failure
├── README.md                # what it is, charts + versions, profiles, connection contract, experiments
└── values/
    ├── operator.yaml        # values for the upstream operator chart (if any)
    ├── small.yaml           # instance chart values — minimum footprint
    ├── ha.yaml              # instance chart values — replicated across zones, for failure experiments
    └── glue.yaml            # bedag/raw values — ONLY for lookup-generated values (credentials)
```

`scripts/stack.py flux` turns `stack.yaml` into `infra/flux/clusters/sdl/stack.generated.yaml`
(gitignored, regenerated on every push): `platform → platform-configs → <name>-operator → <instance> → design`,
each a Flux `Kustomization` with `dependsOn`, `wait: true` and, per instance,
`postBuild.substitute: {INSTANCE, PROFILE}`. Path: `flux/<profile>/` if it exists, else `flux/instance/`.
A profile is valid if `flux/<profile>/` or `values/<profile>.yaml` exists, or it is the default (`small`):
a component with one profile (flink) ships only `flux/instance/`. Removing an entry from
`stack.yaml` + `make sync` prunes its Kustomization, which uninstalls its releases.

Name: lower-kebab, the technology (`kafka`, `redis`, `elasticsearch`, `flink`, `temporal`, `aws`).

## Rules

1. **Flux objects** (`source.toolkit.fluxcd.io/v1`, `helm.toolkit.fluxcd.io/v2`) live in namespace
   `flux-system`; a HelmRelease sets `releaseName` and `targetNamespace` (+ `install.createNamespace` for
   an operator namespace), `install/upgrade.remediation.retries: 3`, and `crds: CreateReplace` when the
   chart ships CRDs. Values: `valuesFrom` a generated ConfigMap (`disableNameSuffixHash: true` + label
   `reconcile.fluxcd.io/watch: Enabled`, so `make sync` upgrades exactly the release whose values
   changed); per-instance overrides in `spec.values`. OCI charts: an `OCIRepository` with the Helm
   `layerSelector` + `chartRef`. Anything named per instance uses `${INSTANCE}` (sources too, so two
   instances never share an object). Quote `${…}` inside YAML flow maps (`{ name: "${INSTANCE}-values" }`);
   annotate objects containing literal `${`/`$1` (JMX rules, relabel configs) with
   `kustomize.toolkit.fluxcd.io/substitute: disabled`.
2. **Pin every version** (chart version / OCI tag + app image) in the Flux objects, with a comment on the
   app version. Look up current versions (helm search / context7 / release pages) — never guess.
3. **Namespaces:** operators in their own namespace (e.g. `strimzi-system`); instances in `data`
   (`data`, `apps`, `k6` are created by the platform).
4. **Placement:** replicated profiles spread over `topology.kubernetes.io/zone` (topologySpreadConstraints
   or the operator's rack/zone awareness). Set requests/limits on everything. Budget: a whole design on
   `small` profiles must fit Docker's 12 GiB minimum (`make doctor`); `ha` may assume ~16 GiB.
5. **Install from existing charts on Artifact Hub.** Prefer the upstream project's own chart/operator,
   then a maintained community chart that runs official images; write chart code only when Artifact Hub
   has nothing suitable. Anything no chart provides (the conn secret, CRs such as `Kafka`, routes) is
   **plain YAML** in the base, in files named by what they hold (`conn.yaml`, `monitoring.yaml`,
   `route.yaml`), applied by the kustomize-controller. **"Glue" means only** a **`bedag/raw`**
   HelmRelease (`<instance>-glue`, `values/glue.yaml`, shared `HelmRepository bedag`), used only when a
   value must be **generated and kept** — its `templates:` run
   through `tpl` in a real `helm upgrade`, so `lookup`, `randAlphaNum`, `genPrivateKey` work (this is
   why the lab uses Flux, not Argo CD). **No Bitnami** charts/images.
6. **Readiness:** `make up` waits until every Kustomization and HelmRelease is Ready, so readiness must be
   real. kstatus ignores a plain `Ready` condition on CRs: give the HelmRelease `healthCheckExprs` for CRs
   it renders (postgres: CNPG `Cluster`); Strimzi CRs applied as plain YAML are covered by the
   `healthCheckExprs` stack.py adds to every generated Kustomization.
7. **Metrics:** if the component exposes Prometheus metrics, add a ServiceMonitor/PodMonitor (any
   namespace is scraped). Grafana dashboards: ConfigMap labelled `grafana_dashboard: "1"`.
8. **UI (optional):** expose tool UIs host-based through the gateway, e.g. `kafka-ui.localhost` —
   HTTPRoute with `parentRefs: [{name: sdl, namespace: envoy-gateway-system}]`. Stateless releases may
   set `driftDetection.mode: enabled` (kafka-ui, aws) so helm-controller undoes manual edits.

## Instances (installing a component more than once)

A `stack.yaml` entry may set `instance:` (default = the component name), e.g. two independent
Postgres clusters:

```yaml
components:
  - { name: postgres, profile: ha }                          # instance "postgres" -> postgres-conn
  - { name: postgres, instance: analytics-db, profile: ha }  # -> analytics-db-conn
```

`make up` / `make sync` reconcile one Kustomization per entry with `INSTANCE` and `PROFILE` substituted;
`make smoke` calls `smoke.sh <instance>` (`make smoke C=<instance>` for one):

- A component that **supports** instances uses it to name its resources (CR / StatefulSet / Services /
  PodMonitor = `<instance>`, hosts `<instance>-….data.svc.cluster.local`) and publishes
  **`apps/<instance>-conn`** with the same keys as the default instance. Services then list
  `connections: [<instance>]` and get env vars prefixed `<INSTANCE>_` (dashes → underscores:
  `analytics-db` → `ANALYTICS_DB_URL`). The default instance must behave exactly as before.
- A component that **doesn't** support instances ships the `flux/single-instance` marker, so
  `stack.py validate` fails fast when the instance isn't the component name. Say so in the README.
  `postgres` is the reference for instances.

## The connection contract (most important)

Every component publishes **one Secret named `<name>-conn` in namespace `apps`**, labelled
`sdl.dev/conn: "true"`, with UPPER_SNAKE keys. Services mount it by listing the component in
`connections:` of their deploy values; the chart turns it into env vars prefixed `<NAME>_`.

| Component     | Secret               | Keys (→ env)                                                                    |
| ------------- | -------------------- | ------------------------------------------------------------------------------- |
| postgres      | `postgres-conn`      | `HOST READ_HOST PORT USER PASSWORD DATABASE URL READ_URL JDBC_URL` → `POSTGRES_URL`, … (`<instance>-conn` per instance) |
| kafka         | `kafka-conn`         | `BOOTSTRAP_SERVERS` (+ `SECURITY_PROTOCOL` if not PLAINTEXT)                    |
| redis         | `redis-conn`         | `URL` (`redis://…`), `MODE` (`standalone`/`cluster`/`sentinel`), `HOST`, `PORT` |
| elasticsearch | `elasticsearch-conn` | `URL`, `USERNAME`, `PASSWORD`                                                   |
| cassandra     | `cassandra-conn`     | `CONTACT_POINTS`, `PORT`, `LOCAL_DC`, `USERNAME`, `PASSWORD`, `KEYSPACE`        |
| temporal      | `temporal-conn`      | `ADDRESS` (`host:7233`), `NAMESPACE`                                            |
| flink         | `flink-conn`         | `REST_URL` (JobManager REST of the design's FlinkDeployment; jobs don't need it) |
| aws           | `aws-conn`           | `ENDPOINT_URL`, `REGION`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` (dummy)          |

Because names are fixed by convention, the architect writes contracts and services code against them
_before_ the component exists. Hosts are always FQDNs (`<svc>.data.svc.cluster.local`) because the
secret is consumed from another namespace. If you need a key not listed here, add it to this table.

## Design-specific setup is NOT part of the component

Topics, buckets, keyspaces, indices, extra databases belong to the design: put them in
`infra/design/flux/` (Kustomization `design`, reconciled after every component instance is Ready: e.g.
Strimzi `KafkaTopic` CRs, a Job that creates buckets, gateway policies). The component stays reusable.

## Done means

- `make up` from scratch is green with the component in `stack.yaml`, and a second `make up`/`make sync`
  changes nothing (`scripts/flux.sh get all -A`)
- `make smoke C=<name>` passes for both profiles you ship
- README documents profiles, the contract keys, and 1–3 failure experiments worth running
- a PLAYBOOK.md row exists/updated with what you learned (gotchas, versions)
