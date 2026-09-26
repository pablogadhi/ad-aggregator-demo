# <Design name> — spec

> Written by the architect step of `/build-design` from `diagram.excalidraw` + `diagram.png`
> (parsed into `diagram.graph.md`). The user approves this file before any building starts.
> Every builder agent treats this file + `contracts/` as the source of truth.

## 1. Problem

One paragraph: what the system does and for whom (from the diagram's notes).

## 2. Requirements

**Functional**

1. …

**Non-functional (as stated)** — availability, latency, consistency, durability, scale.

## 3. Laptop scale-down

The cluster is 4 workers on one machine. Map every scale number to something runnable and say why
the scaled version still exercises the same behaviour.

| Diagram says                                    | Lab target                       | Why it still means something                            |
| ----------------------------------------------- | -------------------------------- | ------------------------------------------------------- |
| e.g. 100M clicks/day (~1.2k rps avg, ~10k peak) | 200 rps sustained, 500 rps burst | same partitioning/back-pressure paths, measurable in k6 |

## 4. Components (infra)

One row per piece of infra. `status: exists` = already in `infra/components/`; `new` = infra-builder
creates it following `infra/components/AUTHORING.md` + `PLAYBOOK.md`.

| Component | Profile | Status | Connection contract (secret `<name>-conn` → env prefix) | Design-specific setup                 |
| --------- | ------- | ------ | ------------------------------------------------------- | ------------------------------------- |
| postgres  | ha      | exists | `POSTGRES_URL`, `POSTGRES_READ_URL`, …                  | tables in `contracts/db/postgres.sql` |

## 5. Services

| Service     | Kind            | Responsibilities | API (contract)                                             | Consumes | Produces  | Stores   |
| ----------- | --------------- | ---------------- | ---------------------------------------------------------- | -------- | --------- | -------- |
| example-api | FastAPI, public | …                | `contracts/openapi/example-api.yaml` at `/api/example-api` | —        | topic `x` | postgres |

Kinds: _FastAPI public_ (HTTPRoute), _FastAPI internal_ (no route), _worker_ (consumer, no public API),
_pipeline_ (Flink job in `pipelines/`).

## 6. Data & events

- DB schemas: `contracts/db/<component>.sql` (owning service copies them into its migrations)
- Events: `contracts/events/<topic>.schema.json` — JSON Schema, plus key, partitions and retention below

| Topic / queue | Key | Partitions | Producer | Consumers | Delivery semantics |
| ------------- | --- | ---------- | -------- | --------- | ------------------ |

## 7. Configuration matrix

`contracts/config.md`: for every service, which connections (`connections:` in its deploy values)
and extra env vars it needs. This is how infra-builder and services-builder agree on wiring.

## 8. Acceptance flows (→ `tests/e2e/`)

Numbered, black-box, through the gateway (`http://localhost:8080`). Each becomes one pytest test.

1. **Given** … **when** … **then** … (use `eventually()` for async/eventually-consistent steps)

## 9. Client

Pages/flows the Next.js demo shows (minimal — just enough to demo the flows above).

## 10. Load (→ `loadtest/<design>.js`)

k6 scenarios + thresholds that encode the scaled NFRs.

## 11. Chaos experiments (→ `chaos/`)

Each NFR that is about failure becomes a hypothesis + experiment + how to verify it.

| Hypothesis                      | Fault                          | Verification                                 |
| ------------------------------- | ------------------------------ | -------------------------------------------- |
| no clicks lost if a broker dies | kill 1 Kafka broker under load | count reconciliation: produced == aggregated |

## 12. Open questions / assumptions

Everything the diagram did not say and the architect had to decide (the user confirms these).
