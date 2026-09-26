#!/usr/bin/env bash
# Removes the Flink operator (FlinkDeployments in apps must be deleted first — pipelines own them).
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
kc -n apps delete secret flink-conn --ignore-not-found >/dev/null
kc delete -k "$HERE/profiles/small" --ignore-not-found >/dev/null
helm uninstall flink-operator --kube-context "$SDL_CLUSTER" -n flink-operator --ignore-not-found >/dev/null
ok "flink operator removed"
