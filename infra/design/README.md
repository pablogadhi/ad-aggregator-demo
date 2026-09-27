# infra/design/ — design-specific infrastructure (ad-aggregator)

Things that belong to _this design_, not to a reusable component. `make up` runs `install.sh`
after all components are installed (it is idempotent; re-run freely).

| File                    | What                                                                                                   | Spec  |
| ----------------------- | ------------------------------------------------------------------------------------------------------ | ----- |
| `topics.yaml`           | `KafkaTopic clicks`: 12 partitions, RF 3, `min.insync.replicas=2`, `retention.ms=86400000` (24 h)       | §6    |
| `buckets-job.yaml`      | Job `apps/create-buckets` (aws-cli + `aws-conn`): S3 bucket **`flink-state`** in Floci                  | §6.1  |
| `install.sh`            | applies the above; creates **Secret `apps/jwt-conn`** once; writes ConfigMap `apps/jwt-jwks`; applies policies | §4.2 |
| `jwks.py`               | stdlib-only PEM → JWKS / RFC 7638 thumbprint (no host installs)                                         | §4.2  |
| `gateway-policies.yaml` | `SecurityPolicy jwt` + `BackendTrafficPolicy resilience` + `BackendTrafficPolicy click-receiver`        | §4.2  |

## jwt-conn (Secret in `apps`, label `sdl.dev/conn=true`)

| Key               | Value                                                                       |
| ----------------- | --------------------------------------------------------------------------- |
| `PRIVATE_KEY_PEM` | RSA 2048 private key, PKCS#8 PEM (`-----BEGIN PRIVATE KEY-----`)            |
| `PUBLIC_KEY_PEM`  | SubjectPublicKeyInfo PEM (`-----BEGIN PUBLIC KEY-----`)                      |
| `KID`             | RFC 7638 SHA-256 JWK thumbprint of the public key (base64url)                |
| `ISSUER`          | `ad-aggregator-auth`                                                         |
| `AUDIENCE`        | `ad-aggregator`                                                              |

Generated on the first `make up` with `openssl` in a one-shot pod (`alpine/openssl:3.5.8`), then
**kept** on every re-run. To rotate: `kubectl -n apps delete secret jwt-conn && make up`, then
restart `auth`. The gateway's JWKS (`ConfigMap apps/jwt-jwks`, key `jwks`) is re-derived from the
secret on every run, so it can't drift from the signing key.

## Gateway policies (namespace `apps`)

- **`SecurityPolicy jwt`** → HTTPRoutes `ad-placement`, `analytics`. Provider `ad-aggregator`:
  `issuer: ad-aggregator-auth`, `audiences: [ad-aggregator]`, **local JWKS** from ConfigMap
  `jwt-jwks` (RS256 key with `kid`), `claimToHeaders`: `sub`→`X-Auth-Sub`, `role`→`X-Auth-Role`,
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
  `infra/platform/gateway.yaml` (EnvoyProxy `sdl-proxy`, 2 replicas zone-spread) for the other half
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
