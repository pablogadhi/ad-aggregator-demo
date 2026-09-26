#!/usr/bin/env bash
# usage: uninstall.sh [instance]  — removes that postgres cluster and its data (the operator stays; it is cheap).
source "$(dirname "$0")/../../../scripts/lib.sh"
INSTANCE=${1:-postgres}
kc -n apps delete secret "$INSTANCE-conn" --ignore-not-found >/dev/null
kc -n data delete "cluster.postgresql.cnpg.io/$INSTANCE" --ignore-not-found --wait >/dev/null
kc -n data delete podmonitor "$INSTANCE" --ignore-not-found >/dev/null
ok "postgres instance $INSTANCE removed"
