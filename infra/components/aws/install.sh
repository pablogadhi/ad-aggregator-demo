#!/usr/bin/env bash
# usage: install.sh [small]   — Floci AWS emulator (S3, SQS, DynamoDB, …) + apps/aws-conn
# (a second arg — the stack.yaml instance — is accepted but must be "aws")
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-aws}
# image pinned in base/floci.yaml: floci/floci:2.1.0

[ -d "$HERE/profiles/$PROFILE" ] || die "aws: unknown profile '$PROFILE' (only 'small')"
[ "$INSTANCE" = aws ] || die "aws: only one instance (named 'aws') is supported, got '$INSTANCE'"

log "aws: applying profile '$PROFILE' (Floci)"
kc apply -k "$HERE/profiles/$PROFILE" >/dev/null
kc -n data rollout status deploy/aws --timeout=300s >/dev/null

# Connection contract: Secret aws-conn in namespace apps. Credentials are dummies (Floci doesn't check them).
kc -n apps create secret generic aws-conn \
  --from-literal=ENDPOINT_URL=http://aws.data.svc.cluster.local:4566 \
  --from-literal=REGION=us-east-1 \
  --from-literal=ACCESS_KEY_ID=test \
  --from-literal=SECRET_ACCESS_KEY=test \
  --dry-run=client -o yaml | kc label --local -f - sdl.dev/conn=true -o yaml | kc apply -f - >/dev/null

ok "aws ($PROFILE) ready — secret apps/aws-conn"
