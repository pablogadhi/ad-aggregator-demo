#!/usr/bin/env bash
# Installs the always-on platform: gateway, cert-manager, metrics-server, monitoring, chaos mesh.
# Idempotent — safe to re-run.
source "$(dirname "$0")/../../scripts/lib.sh"
HERE="$SDL_ROOT/infra/platform"
# shellcheck source=versions.env
source "$HERE/versions.env"

ensure_ns data apps monitoring chaos-mesh

helm_repo jetstack https://charts.jetstack.io
helm_repo prometheus-community https://prometheus-community.github.io/helm-charts
helm_repo chaos-mesh https://charts.chaos-mesh.org
helm_repo metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm repo update jetstack prometheus-community chaos-mesh metrics-server >/dev/null

# Independent charts install in parallel; each writes to its own log.
logs="$SDL_ROOT/.cache/logs"; mkdir -p "$logs"
pids=()
run_bg() { local name=$1; shift; ( "$@" >"$logs/$name.log" 2>&1 ) & pids+=("$!:$name"); }

run_bg envoy-gateway  helm_install eg oci://docker.io/envoyproxy/gateway-helm "$ENVOY_GATEWAY_VERSION" envoy-gateway-system -f "$HERE/values/envoy-gateway.yaml"
run_bg cert-manager   helm_install cert-manager jetstack/cert-manager "$CERT_MANAGER_VERSION" cert-manager --set crds.enabled=true
run_bg metrics-server helm_install metrics-server metrics-server/metrics-server "$METRICS_SERVER_VERSION" kube-system -f "$HERE/values/metrics-server.yaml"
run_bg monitoring     helm_install kps prometheus-community/kube-prometheus-stack "$KUBE_PROMETHEUS_STACK_VERSION" monitoring -f "$HERE/values/kube-prometheus-stack.yaml"
run_bg chaos-mesh     helm_install chaos-mesh chaos-mesh/chaos-mesh "$CHAOS_MESH_VERSION" chaos-mesh -f "$HERE/values/chaos-mesh.yaml"

failed=0
for entry in "${pids[@]}"; do
  pid=${entry%%:*}; name=${entry#*:}
  if wait "$pid"; then ok "$name"; else warn "$name failed — see $logs/$name.log"; tail -20 "$logs/$name.log" >&2; failed=1; fi
done
[ $failed -eq 0 ] || die "platform install failed"

log "gateway: applying GatewayClass/Gateway"
kc apply -f "$HERE/gateway.yaml" >/dev/null
kc -n envoy-gateway-system wait --for=condition=Programmed gateway/sdl --timeout=180s >/dev/null
kc apply -f "$HERE/routes.yaml" >/dev/null
ok "gateway programmed — http://localhost:8080 (grafana.localhost:8080, prometheus.localhost:8080, chaos.localhost:8080)"
