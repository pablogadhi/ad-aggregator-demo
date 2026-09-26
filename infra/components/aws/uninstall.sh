#!/usr/bin/env bash
# Removes Floci and its volume (all buckets/queues/tables).
source "$(dirname "$0")/../../../scripts/lib.sh"
kc -n apps delete secret aws-conn --ignore-not-found >/dev/null
kc -n data delete deploy/aws service/aws pvc/aws-data --ignore-not-found --wait >/dev/null
ok "aws removed"
