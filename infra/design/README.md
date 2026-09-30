# infra/design/ — design-specific infrastructure (ad-aggregator)

Things that belong to _this design_, not to a reusable component: the Flux base `flux/`
(Kustomization `design`, reconciled after every component instance is Ready). Plain YAML, except
`jwt-conn`, a `bedag/raw` 2.0.2 HelmRelease because its key is generated once and kept via `lookup`.

| File                              | What                                                                                              | Spec  |
| --------------------------------- | ------------------------------------------------------------------------------------------------- | ----- |
| `flux/topics.yaml`                | `KafkaTopic clicks` (ns `data`): 12 partitions, RF 3, `min.insync.replicas=2`, `retention.ms=86400000` (24 h) | §6    |
| `flux/buckets.yaml`               | Job `apps/create-buckets` (aws-cli + `aws-conn`, idempotent): S3 bucket **`flink-state`** in Floci | §6.1  |
| `flux/jwt-conn.yaml` + `values/jwt-conn.yaml` | HelmRelease `jwt-conn` → Secret `apps/jwt-conn`: RSA key from `genPrivateKey`, kept via `lookup` | §4.2  |
| `flux/gateway-policies.yaml`      | `SecurityPolicy jwt`, 2 × `BackendTrafficPolicy` (below)                                          | §4.2  |

The bucket Job has no TTL (Flux would recreate it every reconcile) and `kustomize.toolkit.fluxcd.io/force:
enabled` (Flux replaces it when its immutable spec changes). To re-run it: `kubectl -n apps delete job
create-buckets` — Flux recreates it within 2 min.

## jwt-conn (Secret in `apps`, label `sdl.dev/conn=true`)

| Key               | Value                                                                        |
| ----------------- | ---------------------------------------------------------------------------- |
| `PRIVATE_KEY_PEM` | RSA 4096 private key, PKCS#1 PEM (`-----BEGIN RSA PRIVATE KEY-----`, Helm `genPrivateKey "rsa"`) |
| `KID`             | first 16 hex chars of `sha256sum` of the PEM                                  |
| `ISSUER`          | `ad-aggregator-auth`                                                          |
| `AUDIENCE`        | `ad-aggregator`                                                               |

Generated on the first install, then **kept** on every upgrade (`lookup` of the existing secret), so
`KID` never changes across `make up` re-runs. To rotate: `kubectl -n apps delete secret jwt-conn &&
scripts/flux.sh reconcile helmrelease jwt-conn --force`, then restart `auth`. The gateway doesn't need a copy of the public key: it fetches `auth`'s
JWKS (derived from this private key), so the two can't drift.

## Gateway policies (namespace `apps`)

- **`SecurityPolicy jwt`** → HTTPRoutes `ad-placement`, `analytics`. Provider `ad-aggregator`:
  `issuer: ad-aggregator-auth`, `audiences: [ad-aggregator]`, **remote JWKS**
  `uri: http://auth.apps.svc.cluster.local/.well-known/jwks.json` fetched through
  `backendRefs: [{group: "", kind: Service, name: auth, port: 80}]` (EG 1.9.1 CEL rule: "BackendRefs must
  be used, backendRef is not supported"), `cacheDuration: 300s`, `failedRefetchDuration: 5s` (tokens
  are rejected until auth's JWKS has been fetched once), `claimToHeaders`: `sub`→`X-Auth-Sub`, `role`→`X-Auth-Role`,
  `advertiser_id`→`X-Auth-Advertiser-Id`.
- **`BackendTrafficPolicy click-receiver`** → HTTPRoute `click-receiver`: `requestTimeout: 2s`,
  `retry.numRetries: 0`, plus the same passive health check as below.
- **`BackendTrafficPolicy resilience`** → HTTPRoutes `auth`, `ad-placement`, `analytics`: passive
  outlier detection (`healthCheck.passive`: `consecutiveGatewayErrors`/`consecutive5XxErrors: 3`,
  `baseEjectionTime: 30s`, `alwaysEjectOneEndpoint: true`, `maxEjectionPercent: 50`) with
  `healthCheck.panicThreshold: 0` (disables Envoy's panic-route-to-everyone fallback, which would
  otherwise re-include an ejected pod once ≥50% of a small replica set is unhealthy). Ejects dead
  backends from Envoy's own observed failures, independent of how fast xDS/endpoint updates land
  — see the platform-level controller HA in `infra/platform/values/envoy-gateway.yaml` and
  `infra/flux/platform/configs/gateway.yaml` (EnvoyProxy `sdl-proxy`, 2 replicas zone-spread) for the other half
  of the zone-loss fix (chaos #7).

Behaviour verified on the cluster with throwaway routes named `ad-placement` / `click-receiver`
(echo backend, since removed):

| Request                                                        | Result                                                                  |
| -------------------------------------------------------------- | ----------------------------------------------------------------------- |
| no token / garbage / forged payload (bad signature)            | 401                                                                     |
| wrong `iss` / expired                                          | 401                                                                     |
| wrong `aud`                                                    | **403** (Envoy's jwt_authn answers 403 "Audiences in Jwt are not allowed") |
| valid token + spoofed `X-Auth-Advertiser-Id: 999`, `X-Auth-Role`, `X-Auth-Sub` | 200; backend sees only the claim values (`7`, `advertiser`, `advertiser:7`) |
| valid viewer token (`advertiser_id: ""`) + spoofed `X-Auth-Advertiser-Id: 999` | backend sees **no** `X-Auth-Advertiser-Id` (spoofed header stripped, empty claim not forwarded) |
| click-receiver route, 5 s backend                              | 504 after 2 s — **only** because the route itself says 2 s (see below)  |
| click-receiver route, backend 503                              | 503 to the client, backend hit exactly once (no retry)                   |

**Timeout precedence:** an HTTPRoute rule's `timeouts.request` wins over the
BackendTrafficPolicy's `requestTimeout`. The app chart always renders `timeouts.request`
(`route.timeout`, default 15s), so click-receiver's deploy values must keep `route.timeout: 2s`
(they do); the BTP's 2 s then only matters if the route stops setting a timeout.

Services must treat a missing `X-Auth-Advertiser-Id` as "no advertiser" (viewer tokens).
