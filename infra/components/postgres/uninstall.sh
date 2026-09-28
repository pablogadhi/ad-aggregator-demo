#!/usr/bin/env bash
# usage: uninstall.sh [instance]  — removes that postgres cluster and its data (the operator stays; it is cheap).
# CNPG owns the PVCs, so deleting the Cluster deletes them too.
source "$(dirname "$0")/../../../scripts/lib.sh"
INSTANCE=${1:-postgres}
helm uninstall "$INSTANCE" --kube-context "$SDL_CLUSTER" -n data --wait --ignore-not-found >/dev/null
helm uninstall "$INSTANCE-glue" --kube-context "$SDL_CLUSTER" -n data --wait --ignore-not-found >/dev/null
ok "postgres instance $INSTANCE removed (releases $INSTANCE, $INSTANCE-glue)"
