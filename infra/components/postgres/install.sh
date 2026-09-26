#!/usr/bin/env bash
# usage: install.sh [small|ha] [instance]
#   instance (default: postgres) names the CNPG Cluster (services <instance>-rw/-ro), its PodMonitor
#   and the conn secret apps/<instance>-conn — install the component several times for several DBs.
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-postgres}
CNPG_CHART_VERSION=0.29.0   # operator 1.30.0

[ -d "$HERE/profiles/$PROFILE" ] || die "postgres: unknown profile '$PROFILE'"
[[ "$INSTANCE" =~ ^[a-z]([-a-z0-9]*[a-z0-9])?$ ]] || die "postgres: invalid instance name '$INSTANCE' (lower-kebab)"

helm_repo cnpg https://cloudnative-pg.io/charts
helm repo update cnpg >/dev/null
helm_install cnpg cnpg/cloudnative-pg "$CNPG_CHART_VERSION" cnpg-system

log "postgres: applying profile '$PROFILE' as instance '$INSTANCE'"
# The manifests are written for instance "postgres"; rename the Cluster/PodMonitor and the selector.
kc kustomize "$HERE/profiles/$PROFILE" \
  | sed -E -e "s/^(  name: )postgres$/\1$INSTANCE/" -e "s/^(      cnpg\.io\/cluster: )postgres$/\1$INSTANCE/" \
  | kc apply -f - >/dev/null
wait_for "$INSTANCE cluster to be created" 60 kc -n data get "cluster.postgresql.cnpg.io/$INSTANCE"
kc -n data wait --for=condition=Ready "cluster.postgresql.cnpg.io/$INSTANCE" --timeout=600s >/dev/null

# Connection contract: Secret <instance>-conn in namespace apps (see README.md)
wait_for "$INSTANCE-app secret" 60 kc -n data get secret "$INSTANCE-app"
user=$(kc -n data get secret "$INSTANCE-app" -o jsonpath='{.data.username}' | base64 -d)
pass=$(kc -n data get secret "$INSTANCE-app" -o jsonpath='{.data.password}' | base64 -d)
host=$INSTANCE-rw.data.svc.cluster.local
read_host=$INSTANCE-ro.data.svc.cluster.local
enc_pass=$(python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$pass")
kc -n apps create secret generic "$INSTANCE-conn" \
  --from-literal=HOST="$host" \
  --from-literal=READ_HOST="$read_host" \
  --from-literal=PORT=5432 \
  --from-literal=USER="$user" \
  --from-literal=PASSWORD="$pass" \
  --from-literal=DATABASE=app \
  --from-literal=URL="postgresql://$user:$enc_pass@$host:5432/app" \
  --from-literal=READ_URL="postgresql://$user:$enc_pass@$read_host:5432/app" \
  --from-literal=JDBC_URL="jdbc:postgresql://$host:5432/app" \
  --dry-run=client -o yaml | kc label --local -f - sdl.dev/conn=true -o yaml | kc apply -f - >/dev/null

ok "postgres ($PROFILE) ready — instance $INSTANCE, secret apps/$INSTANCE-conn"
