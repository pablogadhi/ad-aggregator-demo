#!/usr/bin/env bash
# usage: install.sh [small|ha] [instance]
#   CloudPirates redis chart, release = instance (default: redis) in namespace data.
#   small: standalone; ha: Redis Cluster 3+3 (formed by the chart's post-install Job, hostname announce).
#   The chart's extraObjects publish apps/<instance>-conn.
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-redis}

REDIS_CHART=oci://registry-1.docker.io/cloudpirates/redis
REDIS_CHART_VERSION=0.35.4   # app 8.10.2: docker.io/redis + oliver006/redis_exporter v1.91.1, digest-pinned by the chart

[ -f "$HERE/values/$PROFILE.yaml" ] || die "redis: unknown profile '$PROFILE'"
[[ "$INSTANCE" =~ ^[a-z]([-a-z0-9]*[a-z0-9])?$ ]] || die "redis: invalid instance name '$INSTANCE' (lower-kebab)"

helm_install "$INSTANCE" "$REDIS_CHART" "$REDIS_CHART_VERSION" data -f "$HERE/values/$PROFILE.yaml"

mode=$(kc -n apps get secret "$INSTANCE-conn" -o jsonpath='{.data.MODE}' | base64 -d)
ok "redis ($PROFILE, $mode) ready — instance $INSTANCE, secret apps/$INSTANCE-conn"
