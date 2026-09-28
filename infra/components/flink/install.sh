#!/usr/bin/env bash
# usage: install.sh [small]   — Apache Flink Kubernetes Operator (watches ns apps) + flink-glue (apps/flink-conn)
# (a second arg — the stack.yaml instance — is accepted but must be "flink")
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-flink}
FLINK_OPERATOR_VERSION=1.16.1   # chart + operator (image ghcr.io/apache/flink-kubernetes-operator, tag set by the chart)
RAW_CHART_VERSION=2.0.2         # bedag/raw (glue: PodMonitors, flink-rest Service, flink-conn)
# Jobs: Flink 2.2.1 (image flink:2.2.1-scala_2.12-java17) — see README for the matching connectors.

[ "$PROFILE" = small ] || die "flink: unknown profile '$PROFILE' (only 'small')"
[ "$INSTANCE" = flink ] || die "flink: only one instance (named 'flink') is supported, got '$INSTANCE'"

# archive.apache.org keeps every release (downloads.apache.org only the latest ones)
helm_repo flink-operator-$FLINK_OPERATOR_VERSION "https://archive.apache.org/dist/flink/flink-kubernetes-operator-$FLINK_OPERATOR_VERSION/"
helm_repo bedag https://bedag.github.io/helm-charts
helm repo update "flink-operator-$FLINK_OPERATOR_VERSION" bedag >/dev/null
# the webhook's certificate comes from cert-manager (installed by the platform)
helm_install flink-operator "flink-operator-$FLINK_OPERATOR_VERSION/flink-kubernetes-operator" "$FLINK_OPERATOR_VERSION" flink-operator \
  -f "$HERE/values/operator.yaml"

helm_install flink-glue bedag/raw "$RAW_CHART_VERSION" apps -f "$HERE/values/glue.yaml"

ok "flink operator ($PROFILE) ready — watches ns apps, secret apps/flink-conn"
