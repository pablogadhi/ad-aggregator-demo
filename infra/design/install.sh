#!/usr/bin/env bash
# Design-specific infra for ad-aggregator (spec §4, §4.2, §6). Idempotent — `make up` runs it after
# the components. Needs: kafka, aws components; Envoy Gateway (platform).
#   - KafkaTopic clicks (12 partitions, RF 3, minISR 2, 24 h)
#   - bucket flink-state in Floci (Job)
#   - Secret apps/jwt-conn: RSA keypair generated once (kept on re-runs) + ConfigMap apps/jwt-jwks
#   - SecurityPolicy jwt (ad-placement, analytics) + BackendTrafficPolicy click-receiver
source "$(dirname "$0")/../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
OPENSSL_IMAGE=alpine/openssl:3.5.8
JWT_ISSUER=ad-aggregator-auth
JWT_AUDIENCE=ad-aggregator

log "design: KafkaTopic clicks"
kc apply -f "$HERE/topics.yaml" >/dev/null
kc -n data wait --for=condition=Ready kafkatopic.kafka.strimzi.io/clicks --timeout=120s >/dev/null
ok "topic clicks: $(kc -n data get kafkatopic clicks -o jsonpath='{.spec.partitions} partitions, RF {.spec.replicas}')"

log "design: S3 bucket flink-state"
kc -n apps delete job create-buckets --ignore-not-found --wait >/dev/null
kc apply -f "$HERE/buckets-job.yaml" >/dev/null
kc -n apps wait --for=condition=complete job/create-buckets --timeout=180s >/dev/null || die "bucket job failed: $(kc -n apps logs job/create-buckets --tail=20)"
ok "bucket flink-state"

if kc -n apps get secret jwt-conn >/dev/null 2>&1; then
  ok "jwt-conn exists — keeping the keypair (delete the secret to rotate)"
else
  log "design: generating the RSA keypair for jwt-conn"
  out=$(run_once apps "$OPENSSL_IMAGE" sh -c \
    'openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out /tmp/k 2>/dev/null && cat /tmp/k && openssl pkey -in /tmp/k -pubout') \
    || die "key generation failed"
  priv=$(echo "$out" | sed -n '/-----BEGIN PRIVATE KEY-----/,/-----END PRIVATE KEY-----/p')
  pub=$(echo "$out" | sed -n '/-----BEGIN PUBLIC KEY-----/,/-----END PUBLIC KEY-----/p')
  [ -n "$priv" ] && [ -n "$pub" ] || die "key generation produced no PEM output"
  kid=$(echo "$pub" | python3 "$HERE/jwks.py" kid)
  kc -n apps create secret generic jwt-conn \
    --from-literal=PRIVATE_KEY_PEM="$priv" \
    --from-literal=PUBLIC_KEY_PEM="$pub" \
    --from-literal=KID="$kid" \
    --from-literal=ISSUER="$JWT_ISSUER" \
    --from-literal=AUDIENCE="$JWT_AUDIENCE" \
    --dry-run=client -o yaml | kc label --local -f - sdl.dev/conn=true -o yaml | kc apply -f - >/dev/null
  ok "jwt-conn created (kid $kid)"
fi

# The gateway's JWKS is always derived from the secret, so it can never drift from the signing key.
pub=$(kc -n apps get secret jwt-conn -o jsonpath='{.data.PUBLIC_KEY_PEM}' | base64 -d)
kid=$(kc -n apps get secret jwt-conn -o jsonpath='{.data.KID}' | base64 -d)
jwks=$(echo "$pub" | python3 "$HERE/jwks.py" jwks "$kid")
kc -n apps create configmap jwt-jwks --from-literal=jwks="$jwks" --dry-run=client -o yaml | kc apply -f - >/dev/null

log "design: gateway policies"
kc apply -f "$HERE/gateway-policies.yaml" >/dev/null
ok "SecurityPolicy jwt (ad-placement, analytics), BackendTrafficPolicy click-receiver (2 s, no retries)"
