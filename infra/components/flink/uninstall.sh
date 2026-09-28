#!/usr/bin/env bash
# Removes the Flink operator + flink-glue (FlinkDeployments in apps must be deleted first — pipelines own them).
source "$(dirname "$0")/../../../scripts/lib.sh"
helm uninstall flink-glue --kube-context "$SDL_CLUSTER" -n apps --ignore-not-found >/dev/null
helm uninstall flink-operator --kube-context "$SDL_CLUSTER" -n flink-operator --ignore-not-found >/dev/null
ok "flink operator removed"
