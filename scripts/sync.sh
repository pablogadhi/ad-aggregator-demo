#!/usr/bin/env bash
# usage: scripts/sync.sh [--bootstrap]   (make sync; make up runs it with --bootstrap)
#
# The local GitOps loop. Flux reconciles the lab from an OCI artifact of infra/ in the kind registry:
#   1. (--bootstrap) helm: flux-operator chart, then release flux-bootstrap (FluxInstance + UI RBAC)
#   2. stack.yaml -> infra/flux/clusters/sdl/stack.generated.yaml (one Kustomization per component entry)
#   3. push infra/ as oci://sdl-registry:5000/sdl-infra:latest (flux-cli image, reproducible digest:
#      unchanged infra/ -> same digest -> nothing to do)
#   4. ask source-controller to fetch it now, then wait until every Kustomization applied that revision
#      and every Kustomization + HelmRelease is Ready.
source "$(dirname "$0")/lib.sh"
# shellcheck source=../infra/flux/bootstrap/versions.env
source "$SDL_ROOT/infra/flux/bootstrap/versions.env"
BOOT="$SDL_ROOT/infra/flux/bootstrap"
TIMEOUT=${SDL_SYNC_TIMEOUT:-1800}

python3 "$SDL_ROOT/scripts/stack.py" validate components || die "fix stack.yaml first"

if [ "${1:-}" = "--bootstrap" ]; then
  helm_install flux-operator "$FLUX_OPERATOR_CHART" "$FLUX_OPERATOR_CHART_VERSION" flux-system -f "$BOOT/flux-operator.yaml"
fi

log "flux: stack.yaml -> infra/flux/clusters/sdl/stack.generated.yaml"
stack flux >"$SDL_ROOT/infra/flux/clusters/sdl/stack.generated.yaml"

sha=$(git -C "$SDL_ROOT" rev-parse HEAD 2>/dev/null || echo 0000000000000000000000000000000000000000)
branch=$(git -C "$SDL_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo local)
log "flux: push infra/ -> $SDL_ARTIFACT:$SDL_ARTIFACT_TAG ($branch@${sha:0:7}$(git -C "$SDL_ROOT" diff --quiet HEAD -- infra stack.yaml 2>/dev/null || echo ', uncommitted changes'))"
# the CLI writes its tarball next to --path, so mount infra/ under the writable /tmp.
# --ignore-paths: push only what Flux reads, so editing docs/scripts doesn't produce a new revision.
out=$(docker run --rm --network kind --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$SDL_ROOT/infra:/tmp/infra:ro" \
  "$FLUX_CLI_IMAGE" push artifact "$SDL_ARTIFACT:$SDL_ARTIFACT_TAG" --path=/tmp/infra \
  --source="$(git -C "$SDL_ROOT" config --get remote.origin.url 2>/dev/null || echo local)" \
  --revision="$branch@sha1:$sha" --insecure-registry --reproducible -o json \
  --ignore-paths='.git/,*.md,*.sh,*.env,cluster/,charts/') || die "flux push failed: $out"
digest=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])' <<<"$out")
ok "pushed $digest"

if [ "${1:-}" = "--bootstrap" ]; then
  helm_repo bedag https://bedag.github.io/helm-charts
  helm repo update bedag >/dev/null
  # helm waits (kstatus) until the FluxInstance is Ready: controllers up, sync source + root Kustomization created
  helm_install flux-bootstrap bedag/raw "$BEDAG_RAW_CHART_VERSION" flux-system -f "$BOOT/flux-instance.yaml"
fi

# fetch the new artifact now instead of at the next interval
kc -n flux-system annotate --overwrite ocirepository/flux-system reconcile.fluxcd.io/requestedAt="$(date +%s)" >/dev/null
wait_for "source flux-system at $digest" 300 \
  bash -c "kubectl --context $SDL_CLUSTER -n flux-system get ocirepository flux-system -o jsonpath='{.status.artifact.revision}' | grep -q '$digest'"
rev=$(kc -n flux-system get ocirepository flux-system -o jsonpath='{.status.artifact.revision}')
ok "source at $rev"

# Wait: every expected Kustomization applied $rev and is Ready; every HelmRelease is Ready (checked
# twice, 5 s apart, so a release that a changed values ConfigMap is about to upgrade isn't missed).
expected=$(stack flux-names)
start=$(date +%s); last=0; stable=0
log "flux: waiting for $(wc -l <<<"$expected") Kustomizations and their HelmReleases (timeout ${TIMEOUT}s)"
while :; do
  ks=$(kc -n flux-system get kustomizations.kustomize.toolkit.fluxcd.io \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.lastAppliedRevision}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null || true)
  hrs=$(kc -n flux-system get helmreleases.helm.toolkit.fluxcd.io \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null || true)
  pending=()
  while read -r name; do
    line=$(awk -v n="$name" '$1 == n' <<<"$ks")
    [[ "$line" == "$name $rev True" ]] || pending+=("ks/$name")
  done <<<"$expected"
  while read -r name status; do
    [ -z "$name" ] || [ "$status" = True ] || pending+=("hr/$name")
  done <<<"$hrs"
  if [ ${#pending[@]} -eq 0 ]; then
    stable=$((stable + 1))
    [ $stable -ge 2 ] && break
  else
    stable=0
  fi
  now=$(date +%s)
  if (( now - start > TIMEOUT )); then
    kc -n flux-system get kustomizations,helmreleases >&2
    die "flux: not Ready after ${TIMEOUT}s: ${pending[*]}"
  fi
  if (( ${#pending[@]} > 0 && now - last >= 30 )); then
    echo "   $((now - start))s, waiting on: ${pending[*]}"; last=$now
  fi
  sleep 5
done
ok "flux: all Kustomizations at ${rev#*@} and all HelmReleases Ready ($(( $(date +%s) - start ))s)"
