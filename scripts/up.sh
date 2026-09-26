#!/usr/bin/env bash
# Creates the cluster (if missing), installs the platform and every component in stack.yaml.
# Idempotent. Services/client are deployed separately by Tilt (make dev / make ci).
source "$(dirname "$0")/lib.sh"

docker info >/dev/null 2>&1 || die "docker is not reachable — run 'make doctor'"
python3 "$SDL_ROOT/scripts/stack.py" validate components || die "fix stack.yaml first"

mkdir -p "$(dirname "$KUBECONFIG")"
log "cluster: ctlptl apply (kind 1 control-plane + 4 workers, registry localhost:5005)"
ctlptl apply -f "$SDL_ROOT/infra/cluster/ctlptl.yaml"
kc wait --for=condition=Ready nodes --all --timeout=300s >/dev/null
ok "nodes ready"
kc get nodes -L topology.kubernetes.io/zone

if [ "${SKIP_PLATFORM:-0}" != "1" ]; then
  "$SDL_ROOT/infra/platform/install.sh"
fi

# each entry: <component> <profile> <instance> (instance defaults to the component name)
while read -r c profile instance; do
  [ -n "$c" ] || continue
  log "component: $instance ($c, $profile)"
  "$SDL_ROOT/infra/components/$c/install.sh" "$profile" "$instance" </dev/null
done < <(stack entries)

# design-specific infra (topics, buckets, extra DBs...) — owned by the design, not the components
if [ -f "$SDL_ROOT/infra/design/install.sh" ]; then
  log "design infra: infra/design/install.sh"
  "$SDL_ROOT/infra/design/install.sh"
elif [ -f "$SDL_ROOT/infra/design/kustomization.yaml" ]; then
  log "design infra: kubectl apply -k infra/design"
  kc apply -k "$SDL_ROOT/infra/design" >/dev/null
fi

ok "lab is up. Next: 'make ci' (deploy + verify services) or 'make dev' (Tilt UI with live rebuilds)"
