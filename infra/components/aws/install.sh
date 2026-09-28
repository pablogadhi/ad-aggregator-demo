#!/usr/bin/env bash
# usage: install.sh [small]   — Floci AWS emulator (quench floci chart) + aws-glue (apps/aws-conn)
# (a second arg — the stack.yaml instance — is accepted but must be "aws")
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-aws}

FLOCI_CHART=oci://ghcr.io/quenchworks/charts/floci
FLOCI_CHART_VERSION=0.2.18   # appVersion 2.1.0; image pinned by DIGEST in the chart (ghcr.io/quenchworks/images/floci)
RAW_CHART_VERSION=2.0.2      # bedag/raw (glue: apps/aws-conn)

[ -f "$HERE/values/$PROFILE.yaml" ] || die "aws: unknown profile '$PROFILE' (only 'small')"
[ "$INSTANCE" = aws ] || die "aws: only one instance (named 'aws') is supported, got '$INSTANCE'"

helm_repo bedag https://bedag.github.io/helm-charts
helm repo update bedag >/dev/null

helm_install aws "$FLOCI_CHART" "$FLOCI_CHART_VERSION" data -f "$HERE/values/$PROFILE.yaml"
helm_install aws-glue bedag/raw "$RAW_CHART_VERSION" apps -f "$HERE/values/glue.yaml"

ok "aws ($PROFILE) ready — secret apps/aws-conn"
