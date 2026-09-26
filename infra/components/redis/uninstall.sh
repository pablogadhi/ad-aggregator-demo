#!/usr/bin/env bash
# Removes Redis and its data (PVCs), so a re-install bootstraps a fresh cluster.
source "$(dirname "$0")/../../../scripts/lib.sh"
kc -n apps delete secret redis-conn --ignore-not-found >/dev/null
kc -n data delete statefulset/redis service/redis service/redis-headless configmap/redis-config podmonitor/redis pdb/redis --ignore-not-found --wait >/dev/null
kc -n data delete pvc -l app.kubernetes.io/name=redis --ignore-not-found >/dev/null
ok "redis removed"
