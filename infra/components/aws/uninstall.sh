#!/usr/bin/env bash
# Removes Floci and its volume (all buckets/queues/tables) and aws-glue.
source "$(dirname "$0")/../../../scripts/lib.sh"
helm uninstall aws-glue --kube-context "$SDL_CLUSTER" -n apps --ignore-not-found >/dev/null
helm uninstall aws --kube-context "$SDL_CLUSTER" -n data --ignore-not-found >/dev/null
ok "aws removed"
