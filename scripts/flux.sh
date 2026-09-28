#!/usr/bin/env bash
# usage: scripts/flux.sh <flux args...>   — the Flux CLI, run from its pinned Docker image (nothing on the host)
#   scripts/flux.sh get all -A
#   scripts/flux.sh suspend kustomization kafka      # before an experiment that patches Flux-managed objects
#   scripts/flux.sh resume kustomization kafka
#   scripts/flux.sh reconcile helmrelease kafka-ui
# Runs on the kind Docker network with kind's internal kubeconfig (the host's 127.0.0.1 API port isn't
# reachable from a container under Docker Desktop).
source "$(dirname "$0")/lib.sh"
# shellcheck source=../infra/flux/bootstrap/versions.env
source "$SDL_ROOT/infra/flux/bootstrap/versions.env"

cfg="$SDL_ROOT/.cache/kubeconfig.internal"
mkdir -p "$(dirname "$cfg")"
kind get kubeconfig --name "$SDL_KIND_NAME" --internal >"$cfg"
exec docker run --rm --network kind --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$cfg:/tmp/kubeconfig:ro" -e KUBECONFIG=/tmp/kubeconfig \
  "$FLUX_CLI_IMAGE" "$@"
