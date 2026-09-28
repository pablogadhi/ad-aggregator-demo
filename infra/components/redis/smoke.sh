#!/usr/bin/env bash
# Proves real behaviour, not just "pods Running":
#   1. redis-conn has URL/MODE/HOST/PORT and a client in namespace apps can SET/GET through it
#   2. (cluster) keys spread over all 3 shards via MOVED redirects; hash-tagged multi-key + Lua work
#   3. (cluster) failover-ready: 3 primaries, each with a connected, acknowledging replica
#   4. (cluster) reports — warns, does not fail — primary/replica pairs that share a zone (the chart's
#      `redis-cli --cluster create` is not zone-aware; accepted trade-off, see README.md)
# usage: smoke.sh [instance]   (default: redis)
source "$(dirname "$0")/../../../scripts/lib.sh"
INSTANCE=${1:-redis}
sel="app.kubernetes.io/instance=$INSTANCE,app.kubernetes.io/name=redis"

get() { kc -n apps get secret "$INSTANCE-conn" -o jsonpath="{.data.$1}" | base64 -d; }
for k in URL MODE HOST PORT; do [ -n "$(get $k)" ] || die "$INSTANCE-conn: missing key $k"; done
url=$(get URL); mode=$(get MODE)
sts=$(kc -n data get statefulset -l "$sel" -o jsonpath='{.items[0].metadata.name}')
[ -n "$sts" ] || die "redis: no StatefulSet for instance $INSTANCE"
image=$(kc -n data get statefulset "$sts" -o jsonpath='{.spec.template.spec.containers[?(@.name=="redis")].image}')
token="smoke$(date +%s)"
c=""; [ "$mode" = cluster ] && c="-c"

log "redis ($mode): SET/GET, hash-tagged MSET/MGET and Lua from namespace apps"
script="set -e
for i in \$(seq 1 30); do redis-cli $c -u $url set sdl:smoke:$token:\$i v\$i ex 600 >/dev/null; done
bad=0; for i in \$(seq 1 30); do [ \"\$(redis-cli $c -u $url get sdl:smoke:$token:\$i)\" = v\$i ] || bad=\$((bad+1)); done
echo getbad=\$bad
redis-cli $c -u $url mset 'sdl:{a:$token}:x' 1 'sdl:{a:$token}:y' 2 >/dev/null
echo mget=\$(redis-cli $c -u $url mget 'sdl:{a:$token}:x' 'sdl:{a:$token}:y' | tr '\n' ,)
echo lua=\$(redis-cli $c -u $url eval \"if redis.call('set', KEYS[1], 1, 'NX', 'EX', 600) then return redis.call('incr', KEYS[2]) end return -1\" 2 'sdl:{a:$token}:flag' 'sdl:{a:$token}:marks')"
out=$(run_once apps "$image" sh -c "$script") || die "redis client ops failed: $out"
echo "$out" | grep -q '^getbad=0$' || die "redis: SET/GET mismatch: $out"
echo "$out" | grep -q '^mget=1,2,$' || die "redis: hash-tagged MSET/MGET failed: $out"
echo "$out" | grep -q '^lua=1$' || die "redis: Lua on single-slot keys failed: $out"
ok "SET/GET x30, MSET/MGET + Lua on {hash-tag} keys via $INSTANCE-conn"

if [ "$mode" = cluster ]; then
  rcli() { local pod=$1; shift; kc -n data exec "$pod" -c redis -- redis-cli "$@" | tr -d '\r'; }
  seed=$sts-0
  info=$(rcli "$seed" cluster info)
  echo "$info" | grep -q cluster_state:ok || die "redis: cluster_state not ok"
  echo "$info" | grep -q cluster_slots_ok:16384 || die "redis: not all slots ok"
  nodes=$(rcli "$seed" cluster nodes)
  # CLUSTER NODES: <id> <ip:port@bus,hostname> <flags> <master-id|-> ...; hostname = <pod>.<headless svc>...
  pod_of() { echo "$1" | awk -F, '{print $2}' | cut -d. -f1; }
  zone_of() { kc get node "$(kc -n data get pod "$1" -o jsonpath='{.spec.nodeName}')" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'; }

  log "redis: keys spread over all shards; replicas attached; zone layout"
  total=0; primaries=0; same_zone=0
  while read -r id addr flags _ <&3; do
    [[ "$flags" == *master* && "$flags" != *fail* ]] || continue
    p=$(pod_of "$addr")
    [ -n "$p" ] || die "redis: node $id announces no hostname (cluster.announceHostnames off?)"
    primaries=$((primaries+1))
    n=$(rcli "$p" --scan --pattern "sdl:smoke:$token:*" | wc -l)
    [ "$n" -gt 0 ] || die "redis: primary $p holds none of the 30 keys (no slot spread?)"
    total=$((total+n))
    acked=$(rcli "$p" wait 1 2000)
    [ "$acked" -ge 1 ] || die "redis: primary $p has no replica acknowledging writes"
    rep=$(pod_of "$(echo "$nodes" | awk -v m="$id" '$4==m && $3 ~ /slave/ && $3 !~ /fail/ {print $2; exit}')")
    [ -n "$rep" ] || die "redis: primary $p has no attached replica"
    [ "$(rcli "$rep" info replication | awk -F: '/master_link_status/{print $2}')" = up ] || die "redis: replica $rep link down"
    pz=$(zone_of "$p"); rz=$(zone_of "$rep")
    if [ "$pz" = "$rz" ]; then
      same_zone=$((same_zone+1))
      warn "redis: $p and its replica $rep are both in $pz (losing $pz loses that shard until it returns)"
    fi
    ok "$p ($pz) holds $n keys; replica $rep ($rz) link up, WAIT acked"
  done 3<<<"$nodes"
  [ "$primaries" -eq 3 ] || die "redis: expected 3 primaries, found $primaries"
  [ "$total" -eq 30 ] || die "redis: expected 30 keys over the primaries, found $total"
  ok "30 keys spread over 3 shards; every primary has a live replica (failover-ready)"
  if [ "$same_zone" -gt 0 ]; then
    warn "redis: $same_zone of 3 primary/replica pairs share a zone (accepted trade-off, not a failure)"
  else
    ok "redis: every replica is in a different zone from its primary"
  fi
fi
ok "redis smoke passed (instance $INSTANCE)"
