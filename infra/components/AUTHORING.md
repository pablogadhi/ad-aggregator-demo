# Authoring a component

A _component_ is a piece of backing infrastructure (Kafka, Redis, Flink, …) that designs select in
`stack.yaml`. Components are built **on demand** the first time a design needs one, then harvested
back into the template (`scripts/harvest-component.sh`) so the next design reuses them.

`postgres/` is the reference implementation — copy its shape.

## Layout

```
infra/components/<name>/
├── install.sh        # usage: install.sh <profile> [instance] — idempotent; only pinned `helm_install` calls:
│                     #   operator release (if any) → glue release → instance release
├── uninstall.sh      # usage: uninstall.sh [instance] — helm uninstall the instance + glue; operators may stay
├── smoke.sh          # usage: smoke.sh [instance] — proves real behaviour; exit non-zero on failure
├── README.md         # what it is, charts + versions, profiles, connection contract, experiments
└── values/
    ├── operator.yaml # values for the upstream operator chart (if any)
    ├── small.yaml    # instance chart values — minimum footprint
    ├── ha.yaml       # instance chart values — replicated across zones, for failure experiments
    └── glue.yaml     # bedag/raw values: the `<instance>-conn` secret and any CRs no chart provides
```

Name: lower-kebab, the technology (`kafka`, `redis`, `elasticsearch`, `flink`, `temporal`, `aws`).

## Rules

1. **Scripts** start with `source "$(dirname "$0")/../../../scripts/lib.sh"` and use its helpers:
   `kc` (kubectl on the lab context), `helm_repo`, `helm_install <release> <chart> <version> <ns>`,
   `wait_for`, `run_once <ns> <image> <cmd…>`, `log/ok/warn/die`.
2. **Pin every version** (chart + app image) as variables at the top of `install.sh`. Look up current
   versions (helm search / context7 / release pages) when creating the component — never guess.
3. **Namespaces:** operators in their own namespace (e.g. `strimzi-system`); instances in `data`.
4. **Placement:** replicated profiles spread over `topology.kubernetes.io/zone` (topologySpreadConstraints
   or the operator's rack/zone awareness). Set requests/limits on everything. Budget: a whole design on
   `small` profiles must fit Docker's 12 GiB minimum (`make doctor`); `ha` may assume ~16 GiB.
5. **Install from existing charts on Artifact Hub, no `kubectl apply`.** Prefer the upstream project's
   own chart/operator, then a maintained community chart that runs official images; write chart code only
   when Artifact Hub has nothing suitable.
   Anything no chart provides (the conn secret, CRs such as `Kafka`, generated credentials) goes in a
   **`bedag/raw`** release named `<instance>-glue` (`values/glue.yaml`; its `templates:` run through `tpl`,
   so `.Release.Name`, `lookup`, `randAlphaNum`, `genPrivateKey` work — use `lookup` to keep generated
   values stable across upgrades). The release name is the instance. **No Bitnami** charts/images (moved
   to a legacy, unmaintained catalog in Aug 2025).
6. **Metrics:** if the component exposes Prometheus metrics, add a ServiceMonitor/PodMonitor (any
   namespace is scraped). Grafana dashboards: ConfigMap labelled `grafana_dashboard: "1"`.
7. **UI (optional):** expose tool UIs host-based through the gateway, e.g. `kafka-ui.localhost` —
   HTTPRoute with `parentRefs: [{name: sdl, namespace: envoy-gateway-system}]`.

## Instances (installing a component more than once)

A `stack.yaml` entry may set `instance:` (default = the component name), e.g. two independent
Postgres clusters:

```yaml
components:
  - { name: postgres, profile: ha }                          # instance "postgres" -> postgres-conn
  - { name: postgres, instance: analytics-db, profile: ha }  # -> analytics-db-conn
```

`make up` calls `install.sh <profile> <instance>`, `make smoke` calls `smoke.sh <instance>` (`make smoke
C=<instance>` for one). The instance is always passed, so every component receives the 2nd argument:

- A component that **supports** instances uses it to name its resources (CR / StatefulSet / Services /
  PodMonitor = `<instance>`, hosts `<instance>-….data.svc.cluster.local`) and publishes
  **`apps/<instance>-conn`** with the same keys as the default instance. Services then list
  `connections: [<instance>]` and get env vars prefixed `<INSTANCE>_` (dashes → underscores:
  `analytics-db` → `ANALYTICS_DB_URL`). The default instance must behave exactly as before.
- A component that **doesn't** support instances may ignore the argument, or (better) fail fast if
  it isn't the component name. Say which in the README. `postgres` is the reference for instances.

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
`infra/design/` (applied by infra-builder after components are up, e.g. Strimzi `KafkaTopic` CRs,
a Job that creates buckets). The component stays reusable across designs.

## Done means

- `infra/components/<name>/install.sh <profile>` works on a fresh `make up` and when re-run
- `make smoke C=<name>` passes for both profiles you ship
- README documents profiles, the contract keys, and 1–3 failure experiments worth running
- a PLAYBOOK.md row exists/updated with what you learned (gotchas, versions)
