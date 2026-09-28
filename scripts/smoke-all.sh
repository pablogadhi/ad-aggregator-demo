#!/usr/bin/env bash
# usage: smoke-all.sh [component|instance] — runs smoke.sh for one component/instance or all in stack.yaml
source "$(dirname "$0")/lib.sh"

log "platform: gateway reachable"
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: grafana.localhost' "$SDL_GATEWAY_URL/api/health" || true)
[ "$code" = "200" ] || die "gateway/grafana not reachable at $SDL_GATEWAY_URL (HTTP $code)"
ok "gateway -> grafana"
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: flux.localhost' "$SDL_GATEWAY_URL/" || true)
[ "$code" = "200" ] || die "Flux web UI not reachable at flux.localhost (HTTP $code)"
ok "gateway -> flux web UI"

entries=$(stack entries ${1:+"$1"})
[ -n "$entries" ] || die "no component or instance named '$1' in stack.yaml"
while read -r c _profile instance; do
  "$SDL_ROOT/infra/components/$c/smoke.sh" "$instance" </dev/null
done <<< "$entries"
ok "all smoke tests passed"
