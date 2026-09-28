#!/usr/bin/env bash
# Creates the cluster (if missing), bootstraps Flux and lets it install the platform, every component in
# stack.yaml and infra/design/. Idempotent. Services/client are deployed separately by Tilt (make dev / make ci).
source "$(dirname "$0")/lib.sh"

docker info >/dev/null 2>&1 || die "docker is not reachable — run 'make doctor'"
python3 "$SDL_ROOT/scripts/stack.py" validate components || die "fix stack.yaml first"

mkdir -p "$(dirname "$KUBECONFIG")"
log "cluster: ctlptl apply (kind 1 control-plane + 4 workers, registry localhost:5005)"
ctlptl apply -f "$SDL_ROOT/infra/cluster/ctlptl.yaml"
kc wait --for=condition=Ready nodes --all --timeout=300s >/dev/null
ok "nodes ready"
kc get nodes -L topology.kubernetes.io/zone

# Everything else (platform charts, components from stack.yaml, infra/design/) is reconciled by Flux from
# an OCI artifact of infra/ — see scripts/sync.sh (also `make sync` after editing infra/ or stack.yaml).
"$SDL_ROOT/scripts/sync.sh" --bootstrap

ok "lab is up (Flux UI: http://flux.localhost:8080). Next: 'make ci' (deploy + verify services) or 'make dev' (Tilt UI with live rebuilds)"
