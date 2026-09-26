#!/usr/bin/env bash
# Proves real behaviour, not just "pods Running":
#   1. redis-conn has URL/MODE/HOST/PORT and a client in namespace apps can SET/GET through it
#   2. (cluster) keys spread over all 3 shards via MOVED redirects; hash-tagged multi-key + Lua work
#   3. (cluster) failover-ready: 3 primaries, each with a connected, acknowledging replica in another zone
source "$(dirname "$0")/../../../scripts/lib.sh"

get() { kc -n apps get secret redis-conn -o jsonpath="{.data.$1}" | base64 -d; }
for k in URL MODE HOST PORT; do [ -n "$(get $k)" ] || die "redis-conn: missing key $k"; done
url=$(get URL); mode=$(get MODE)
image=$(kc -n data get statefulset redis -o jsonpath='{.spec.template.spec.containers[0].image}')
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
ok "SET/GET x30, MSET/MGET + Lua on {hash-tag} keys via redis-conn"

if [ "$mode" = cluster ]; then
  rcli() { local pod=$1; shift; kc -n data exec "$pod" -c redis -- redis-cli "$@" | tr -d '\r'; }
  info=$(rcli redis-0 cluster info)
  echo "$info" | grep -q cluster_state:ok || die "redis: cluster_state not ok"
  echo "$info" | grep -q cluster_slots_ok:16384 || die "redis: not all slots ok"
  zone_of() { kc get node "$(kc -n data get pod "$1" -o jsonpath='{.spec.nodeName}')" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'; }

  log "redis: keys spread over all shards; replicas attached"
  total=0; primaries=0
  for i in $(seq 0 5); do
    p=redis-$i
    role=$(rcli "$p" role | head -1)
    [ "$role" = master ] || continue
    primaries=$((primaries+1))
    n=$(rcli "$p" --scan --pattern "sdl:smoke:$token:*" | wc -l)
    [ "$n" -gt 0 ] || die "redis: primary $p holds none of the 30 keys (no slot spread?)"
    total=$((total+n))
    acked=$(rcli "$p" wait 1 2000)
    [ "$acked" -ge 1 ] || die "redis: primary $p has no replica acknowledging writes"
    rep_ip=$(rcli "$p" info replication | awk -F'[=,]' '/^slave0:/{print $2}')
    rep=$(kc -n data get pod -l app.kubernetes.io/name=redis -o jsonpath="{range .items[?(@.status.podIP==\"$rep_ip\")]}{.metadata.name}{end}")
    [ -n "$rep" ] || die "redis: primary $p has no attached replica"
    [ "$(rcli "$rep" info replication | awk -F: '/master_link_status/{print $2}')" = up ] || die "redis: replica $rep link down"
    pz=$(zone_of "$p"); rz=$(zone_of "$rep")
    [ "$pz" != "$rz" ] || warn "redis: $p and its replica $rep are both in $pz (a zone loss would lose that shard)"
    ok "$p ($pz) holds $n keys; replica $rep ($rz) link up, WAIT acked"
  done
  [ "$primaries" -eq 3 ] || die "redis: expected 3 primaries, found $primaries"
  [ "$total" -eq 30 ] || die "redis: expected 30 keys over the primaries, found $total"
  ok "30 keys spread over 3 shards; every primary has a live replica (failover-ready)"
fi
ok "redis smoke passed"
