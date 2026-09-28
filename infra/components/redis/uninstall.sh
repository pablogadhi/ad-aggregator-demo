#!/usr/bin/env bash
# usage: uninstall.sh [instance]  — removes that Redis release and its data (PVC retention policy
# whenDeleted: Delete), so a re-install forms a fresh cluster.
source "$(dirname "$0")/../../../scripts/lib.sh"
INSTANCE=${1:-redis}
helm uninstall "$INSTANCE" --kube-context "$SDL_CLUSTER" -n data --wait --ignore-not-found >/dev/null
kc -n data delete pvc -l "app.kubernetes.io/instance=$INSTANCE,app.kubernetes.io/name=redis" --ignore-not-found >/dev/null
ok "redis instance $INSTANCE removed"
