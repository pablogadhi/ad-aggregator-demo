# System Design Lab — agent guide

A local, multi-node Kubernetes lab for building system designs end to end: infra components,
FastAPI services, stream pipelines and a thin Next.js client, verified with e2e tests, load tests
and chaos experiments. Nothing is deployed to a cloud. **One design per repo** (created with
`scripts/new-design.sh` from the template; `origin` = the template).

To build a design from a diagram, run the **`/build-design`** skill (`.claude/skills/build-design/`).

## Hard rules

- **Never install anything on the host** (no brew/apt/pip --user/npm -g/curl|sh, no sysctl, no
  changing Docker Desktop settings). If a tool or setting is missing, run `make doctor` and report
  what's missing to the user. Everything else (images, charts, venvs, node_modules) lives in Docker
  or inside the repo.
- **Kubeconfig, helm and tilt state are repo-local** (`scripts/env.sh`): always use the Makefile /
  scripts, or `source scripts/env.sh` first. Never touch `~/.kube/config`. Context: `kind-sdl`.
- **Contracts first.** `design/spec.md` + `design/contracts/` are the source of truth shared by every
  agent. Change a contract only through the orchestrator (it re-dispatches everyone affected).
- **Stay in your lane:** each builder agent writes only the paths it owns (table below).
- Pin versions; look them up (context7, `helm search repo`, release pages) — don't guess.

## Layout and ownership

| Path                                | What                                                                                    | Owner                               |
| ----------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------- |
| `design/`                           | diagram (input), `spec.md`, `contracts/` (openapi, events, db, config.md), `RESULTS.md` | architect (orchestrator)            |
| `stack.yaml`                        | which components / services / pipelines / client this design runs                       | architect                           |
| `infra/cluster/`                    | kind cluster (1 control plane + 4 workers, zones a/b/c) + local registry                | template                            |
| `infra/flux/`                       | Flux bootstrap (operator + `FluxInstance`), root `clusters/sdl/`, platform HelmReleases + glue | template                            |
| `infra/platform/values/`            | values for Envoy Gateway, cert-manager, metrics-server, Prometheus/Grafana, Chaos Mesh  | template                            |
| `infra/components/<name>/`          | reusable backing infra (postgres, kafka, …) — `AUTHORING.md`, `PLAYBOOK.md`             | infra-builder                       |
| `infra/design/`                     | design-specific infra: topics, buckets, extra DBs, gateway policies                     | infra-builder                       |
| `infra/charts/app/`                 | generic chart for every service + the client                                            | template (infra-builder may extend) |
| `services/`                         | uv workspace: `libs/common` (sdl_common), `_template`, one folder per service           | services-builder                    |
| `pipelines/<name>/`                 | stream jobs (Flink) with their own `Tiltfile`                                           | services-builder                    |
| `client/`                           | Next.js demo app                                                                        | client-builder                      |
| `tests/e2e/`, `loadtest/`, `chaos/` | acceptance flows, k6 scripts, chaos experiments                                         | integrator                          |

## Commands

|                                                                                         |                                                                                   |
| --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `make doctor`                                                                           | read-only environment check                                                       |
| `make up` / `make down`                                                                 | cluster + Flux, which installs platform + `stack.yaml` components + `infra/design/` / delete cluster |
| `make sync`                                                                             | after editing `infra/` or `stack.yaml`: push `infra/` to Flux, wait until reconciled |
| `scripts/flux.sh <args>`                                                                | Flux CLI from its Docker image: `get all -A`, `suspend kustomization kafka`, …    |
| `make smoke [C=<component>]`                                                            | component smoke tests (real reads/writes, not just "Running")                     |
| `make test`                                                                             | unit tests for all services + scripts (no cluster)                                |
| `make ci`                                                                               | Tilt headless: build + deploy services/pipelines/client, then run `tests/e2e`     |
| `make dev`                                                                              | Tilt UI at <http://localhost:10350> with rebuild-on-change                        |
| `make e2e` · `make load [S=loadtest/x.js]` · `make chaos E=<name>` · `make chaos-clear` |                                                                                   |
| `make new-service N=<name>`                                                             | scaffold a FastAPI service from `services/_template`                              |
| `make graph`                                                                            | `design/diagram.excalidraw` → `design/diagram.graph.md`                           |
| `make harvest C=<component>`                                                            | push a new component back to the template as branch `component/<name>`            |

URLs: app `http://localhost:8080` (services at `/api/<service>/…`), `grafana.localhost:8080`
(admin/admin), `prometheus.localhost:8080`, `chaos.localhost:8080`, `flux.localhost:8080` (Flux web UI).

## Conventions

- **Namespaces:** components in `data`, services + client in `apps`, operators in their own.
- **Connection contract:** component `<name>` publishes Secret `apps/<name>-conn`; a service lists
  it under `connections:` in `services/<svc>/deploy/values.yaml` and gets env vars `<NAME>_<KEY>`
  (e.g. `POSTGRES_URL`, `KAFKA_BOOTSTRAP_SERVERS`). Keys per component: `infra/components/AUTHORING.md`.
- **Services** (see `services/sample-api` as the reference):
  - FastAPI via `sdl_common.create_app(settings, lifespan=…, readiness=…)` → `/healthz` (liveness,
    never checks deps), `/readyz` (deps), `/metrics`, JSON logs, `X-Served-By: pod@node` header.
  - Entry point `python -m <module>`; one shared `services/Dockerfile` (`SERVICE` build arg).
  - Public routes: `route.pathPrefix: /api/<service>` (prefix stripped; `ROOT_PATH` set for docs).
    Workers: `route.enabled: false`.
  - Keep I/O behind a small repo/client class; unit tests swap it via `app.dependency_overrides`.
  - `test_matches_contract` in every public service: `assert_implements_contract(app, "<service>")`.
  - SQL migrations: `src/<module>/migrations/NNNN_name.sql`, run by `sdl_common.migrate` as an init
    container (`migrations.enabled: true` in deploy values).
- **Client:** relative `fetch('/api/<service>/…')` through `src/lib/api.ts`; types generated from
  the contracts with `pnpm gen:api` (commit `src/api/*.ts`).
- **Replicas & zones:** the chart spreads replicas across zones and adds a PDB; stateful components
  spread via their operator. Design experiments around zones (`chaos/`, `chaos/node-down.sh`).
- **E2E tests** go through the gateway only (black box); use `eventually()` for async flows.
- Only one lab cluster runs at a time on this machine (`kind-sdl`); `make down` before switching designs.
- Docker Desktop: the host cannot reach container IPs; everything is exposed through the gateway's
  NodePort mapped to `localhost:8080`.

## Gotchas learned the hard way

- **Flux owns `infra/`** (platform, components, design): `make up` bootstraps it, `make sync` pushes edits
  (an OCI artifact in the kind registry, no Git remote). Manual `kubectl`/`helm` changes to those objects
  are reverted — `scripts/flux.sh suspend kustomization <name>` first (see `chaos/README.md`). Services,
  pipelines and the client stay on Tilt.

- Commands that need Docker or the cluster (`make up/ci/e2e/smoke/load/chaos…`, `kubectl`, `helm`,
  `docker`, `kind`, `tilt`, `k6`) run outside the Claude Code sandbox via `excludedCommands` in
  `.claude/settings.json`; inside it there is no Docker socket and no host `localhost`. Run them
  bare: a pipe, `cd`, `&&` chain or redirect keeps the whole call sandboxed and it fails.

- `kubectl run -i` can drop output of fast commands — use `run_once` from `scripts/lib.sh`.
- NetworkChaos with `direction: to` does nothing for traffic to Services — use `direction: both`
  (details in `chaos/README.md`).
- Don't bump or remove `packageManager: pnpm@12.3.4` in `client/package.json`: `pnpm` here is a
  Corepack shim, and any other version makes it try to download pnpm, which fails offline/sandboxed.
