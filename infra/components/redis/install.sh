#!/usr/bin/env bash
# usage: install.sh [small|ha]   — Redis 8 StatefulSet (small: standalone, ha: Redis Cluster 3+3) + apps/redis-conn
# (a second arg — the stack.yaml instance — is accepted but must be "redis")
source "$(dirname "$0")/../../../scripts/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${1:-small}
INSTANCE=${2:-redis}
# images are pinned in base/statefulset.yaml: redis:8.8.3, oliver006/redis_exporter:v1.92.0

[ -d "$HERE/profiles/$PROFILE" ] || die "redis: unknown profile '$PROFILE'"
[ "$INSTANCE" = redis ] || die "redis: only one instance (named 'redis') is supported, got '$INSTANCE'"

log "redis: applying profile '$PROFILE'"
kc apply -k "$HERE/profiles/$PROFILE" >/dev/null
kc -n data rollout status statefulset/redis --timeout=600s >/dev/null
mode=$(kc -n data get statefulset redis -o jsonpath='{.spec.template.spec.containers[0].env[0].value}')
replicas=$(kc -n data get statefulset redis -o jsonpath='{.spec.replicas}')

rcli() { local pod=$1; shift; kc -n data exec "$pod" -c redis -- redis-cli "$@"; }

if [ "$mode" = cluster ]; then
  [ "$replicas" -eq 6 ] || die "redis: cluster bootstrap expects 6 pods, got $replicas"
  known=$(rcli redis-0 cluster info | tr -d '\r' | awk -F: '/cluster_known_nodes/{print $2}')
  if [ "$known" -ge 6 ]; then
    ok "redis cluster already formed ($known nodes)"
  else
    log "redis: creating the cluster (one primary per zone, replicas in another zone)"
    # pod -> zone -> ip
    declare -A zone ip
    for i in $(seq 0 5); do
      p=redis-$i
      node=$(kc -n data get pod "$p" -o jsonpath='{.spec.nodeName}')
      zone[$p]=$(kc get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
      ip[$p]=$(kc -n data get pod "$p" -o jsonpath='{.status.podIP}')
    done
    primaries=(); declare -A seen
    for i in $(seq 0 5); do p=redis-$i
      if [ -z "${seen[${zone[$p]}]:-}" ] && [ ${#primaries[@]} -lt 3 ]; then primaries+=("$p"); seen[${zone[$p]}]=1; fi
    done
    for i in $(seq 0 5); do p=redis-$i   # fewer than 3 zones: fill up in pod order
      [ ${#primaries[@]} -ge 3 ] && break
      [[ " ${primaries[*]} " == *" $p "* ]] || primaries+=("$p")
    done
    replicas_list=()
    for i in $(seq 0 5); do p=redis-$i; [[ " ${primaries[*]} " == *" $p "* ]] || replicas_list+=("$p"); done

    rcli redis-0 --cluster create "${ip[${primaries[0]}]}:6379" "${ip[${primaries[1]}]}:6379" "${ip[${primaries[2]}]}:6379" \
      --cluster-replicas 0 --cluster-yes >/dev/null
    wait_for "3 primaries to agree on the slot map" 60 sh -c "kubectl --context $SDL_CLUSTER -n data exec ${primaries[0]} -c redis -- redis-cli cluster info | grep -q cluster_state:ok"

    # pair replicas with primaries: the permutation with the fewest same-zone pairs wins
    best=""; best_conflicts=99
    for perm in "0 1 2" "0 2 1" "1 0 2" "1 2 0" "2 0 1" "2 1 0"; do
      read -r a b c <<<"$perm"; idx=("$a" "$b" "$c"); conflicts=0
      for k in 0 1 2; do
        [ "${zone[${replicas_list[$k]}]}" = "${zone[${primaries[${idx[$k]}]}]}" ] && conflicts=$((conflicts+1))
      done
      if [ "$conflicts" -lt "$best_conflicts" ]; then best=$perm; best_conflicts=$conflicts; fi
    done
    read -r a b c <<<"$best"; idx=("$a" "$b" "$c")
    for k in 0 1 2; do
      r=${replicas_list[$k]}; target=${primaries[${idx[$k]}]}
      pid=$(rcli "$target" cluster myid | tr -d '\r')
      log "redis: $r (${zone[$r]}) replicates $target (${zone[$target]})"
      rcli redis-0 --cluster add-node "${ip[$r]}:6379" "${ip[${primaries[0]}]}:6379" \
        --cluster-slave --cluster-master-id "$pid" >/dev/null
    done
  fi
  wait_for "redis cluster: 6 nodes, 3 primaries each with a replica" 180 sh -c "
    info=\$(kubectl --context $SDL_CLUSTER -n data exec redis-0 -c redis -- redis-cli cluster info)
    echo \"\$info\" | grep -q cluster_state:ok && echo \"\$info\" | grep -q cluster_known_nodes:6 &&
    [ \$(kubectl --context $SDL_CLUSTER -n data exec redis-0 -c redis -- redis-cli cluster nodes | grep -c 'slave.*connected') -eq 3 ]"
  ok "redis cluster: state ok, 3 primaries + 3 replicas"
fi

# Connection contract: Secret redis-conn in namespace apps
host=redis.data.svc.cluster.local
kc -n apps create secret generic redis-conn \
  --from-literal=URL="redis://$host:6379" \
  --from-literal=MODE="$mode" \
  --from-literal=HOST="$host" \
  --from-literal=PORT=6379 \
  --dry-run=client -o yaml | kc label --local -f - sdl.dev/conn=true -o yaml | kc apply -f - >/dev/null

ok "redis ($PROFILE, $mode) ready — secret apps/redis-conn"
