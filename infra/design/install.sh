#!/usr/bin/env bash
# Design-specific infra for ad-aggregator (spec §4, §4.2, §6). Idempotent — `make up` runs it after the
# components. Needs: kafka, aws components; Envoy Gateway (platform). One bedag/raw release `design` in apps:
#   - KafkaTopic clicks (12 partitions, RF 3, minISR 2, 24 h)
#   - bucket flink-state in Floci (post-install/upgrade hook Job)
#   - Secret apps/jwt-conn: RSA key generated once, kept on re-runs (lookup)
#   - SecurityPolicy jwt (remote JWKS from auth) + BackendTrafficPolicies resilience, click-receiver
source "$(dirname "$0")/../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
RAW_CHART_VERSION=2.0.2   # bedag/raw

helm_repo bedag https://bedag.github.io/helm-charts
helm repo update bedag >/dev/null
helm_install design bedag/raw "$RAW_CHART_VERSION" apps -f "$HERE/values/glue.yaml"
ok "design: topic clicks, bucket flink-state, jwt-conn (kid $(kc -n apps get secret jwt-conn -o jsonpath='{.data.KID}' | base64 -d)), gateway policies"
