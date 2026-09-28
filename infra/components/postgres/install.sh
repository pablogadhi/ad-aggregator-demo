#!/usr/bin/env bash
# usage: install.sh [small|ha] [instance]
#   instance (default: postgres) is the cnpg/cluster release name: it names the CNPG Cluster (services
#   <instance>-rw/-ro), its PodMonitor and the conn secret apps/<instance>-conn — install the component
#   several times for several DBs. Releases: cnpg (operator) -> <instance>-glue (credentials + conn
#   secret, bedag/raw) -> <instance> (cnpg/cluster).
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-postgres}

CNPG_OPERATOR_CHART_VERSION=0.29.0   # cnpg/cloudnative-pg, operator 1.30.0
CNPG_CLUSTER_CHART_VERSION=0.8.1     # cnpg/cluster (image pinned in values/<profile>.yaml)
RAW_CHART_VERSION=2.0.2              # bedag/raw (glue)

[ -f "$HERE/values/$PROFILE.yaml" ] || die "postgres: unknown profile '$PROFILE'"
[[ "$INSTANCE" =~ ^[a-z]([-a-z0-9]*[a-z0-9])?$ ]] || die "postgres: invalid instance name '$INSTANCE' (lower-kebab)"

helm_repo cnpg https://cloudnative-pg.github.io/charts
helm_repo bedag https://bedag.github.io/helm-charts
helm repo update cnpg bedag >/dev/null

helm_install cnpg cnpg/cloudnative-pg "$CNPG_OPERATOR_CHART_VERSION" cnpg-system
helm_install "$INSTANCE-glue" bedag/raw "$RAW_CHART_VERSION" data -f "$HERE/values/glue.yaml"
helm_install "$INSTANCE" cnpg/cluster "$CNPG_CLUSTER_CHART_VERSION" data -f "$HERE/values/$PROFILE.yaml" \
  --set fullnameOverride="$INSTANCE" --set cluster.initdb.secret.name="$INSTANCE-app-credentials"
kc -n data wait --for=condition=Ready "cluster.postgresql.cnpg.io/$INSTANCE" --timeout=600s >/dev/null

ok "postgres ($PROFILE) ready — instance $INSTANCE, secret apps/$INSTANCE-conn"
