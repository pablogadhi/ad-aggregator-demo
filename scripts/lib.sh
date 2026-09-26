# shellcheck shell=bash
# Shared helpers for lab scripts. Source after env.sh.

set -euo pipefail

# shellcheck source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m !!\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31mERR\033[0m %s\n' "$*" >&2; exit 1; }

kc() { kubectl --context "$SDL_CLUSTER" "$@"; }

# helm_install <release> <chart> <version> <namespace> [extra helm args...]
helm_install() {
  local release=$1 chart=$2 version=$3 ns=$4; shift 4
  log "helm: $release ($chart $version) -> ns/$ns"
  helm upgrade --install "$release" "$chart" --version "$version" \
    --kube-context "$SDL_CLUSTER" --namespace "$ns" --create-namespace \
    --wait --timeout 10m "$@"
}

# helm_repo <name> <url>  (idempotent, repo-local config)
helm_repo() {
  helm repo list 2>/dev/null | awk '{print $1}' | grep -qx "$1" || helm repo add "$1" "$2" >/dev/null
}

ensure_ns() {
  local ns
  for ns in "$@"; do
    kc get ns "$ns" >/dev/null 2>&1 || kc create ns "$ns" >/dev/null
  done
}

# wait_for <description> <timeout-seconds> <command...>  — retries command until it succeeds
wait_for() {
  local desc=$1 timeout=$2; shift 2
  local start; start=$(date +%s)
  until "$@" >/dev/null 2>&1; do
    if (( $(date +%s) - start > timeout )); then die "timed out waiting for: $desc"; fi
    sleep 3
  done
}

# run_once <namespace> <image> <cmd...> — runs a one-shot pod, prints its logs, returns its exit status.
# (kubectl run -i races with fast commands and can drop output; this waits for completion instead.)
run_once() {
  local ns=$1 image=$2; shift 2
  local name="sdl-once-$RANDOM$RANDOM" phase=""
  kc -n "$ns" run "$name" --restart=Never --image="$image" --command -- "$@" >/dev/null
  for _ in $(seq 1 120); do
    phase=$(kc -n "$ns" get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == Succeeded || "$phase" == Failed ]] && break
    sleep 1
  done
  kc -n "$ns" logs "$name" 2>&1
  kc -n "$ns" delete pod "$name" --wait=false >/dev/null 2>&1
  [ "$phase" = Succeeded ]
}

# stack <query> — reads stack.yaml (components | entries [name] | profile <name> | services | client)
stack() { python3 "$SDL_ROOT/scripts/stack.py" "$@"; }
