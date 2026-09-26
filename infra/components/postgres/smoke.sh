#!/usr/bin/env bash
# Proves real behaviour, not just "pods Running":
#   1. a client in the apps namespace can write + read through postgres-conn (the connection contract)
#   2. (ha) every replica is streaming from the primary, and a replica serves reads via READ_URL
#   3. the conn secret carries every contract key (incl. JDBC_URL)
# usage: smoke.sh [instance]   (default: postgres)
source "$(dirname "$0")/../../../scripts/lib.sh"
INSTANCE=${1:-postgres}

instances=$(kc -n data get cluster.postgresql.cnpg.io/$INSTANCE -o jsonpath='{.spec.instances}')
image=$(kc -n data get cluster.postgresql.cnpg.io/$INSTANCE -o jsonpath='{.status.image}')
url=$(kc -n apps get secret "$INSTANCE-conn" -o jsonpath='{.data.URL}' | base64 -d)
read_url=$(kc -n apps get secret "$INSTANCE-conn" -o jsonpath='{.data.READ_URL}' | base64 -d)

token="smoke-$(date +%s)"
sql="CREATE TABLE IF NOT EXISTS sdl_smoke(id serial primary key, token text, at timestamptz default now());
     INSERT INTO sdl_smoke(token) VALUES ('$token');
     SELECT token FROM sdl_smoke WHERE token = '$token';"

for key in HOST READ_HOST PORT USER PASSWORD DATABASE URL READ_URL JDBC_URL; do
  [ -n "$(kc -n apps get secret "$INSTANCE-conn" -o jsonpath="{.data.$key}")" ] || die "$INSTANCE-conn: missing key $key"
done
jdbc=$(kc -n apps get secret "$INSTANCE-conn" -o jsonpath='{.data.JDBC_URL}' | base64 -d)
[ "$jdbc" = "jdbc:postgresql://$INSTANCE-rw.data.svc.cluster.local:5432/app" ] || die "$INSTANCE-conn: unexpected JDBC_URL $jdbc"
ok "$INSTANCE-conn has all contract keys"

log "$INSTANCE: write/read from namespace apps"
out=$(run_once apps "$image" psql "$url" -tA -v ON_ERROR_STOP=1 -c "$sql") || die "postgres write/read failed: $out"
echo "$out" | grep -q "$token" || die "postgres: wrote $token but did not read it back: $out"
ok "write/read via $INSTANCE-conn"

if [ "${instances:-1}" -gt 1 ]; then
  log "$INSTANCE: replication"
  primary=$(kc -n data get pod -l cnpg.io/cluster=$INSTANCE,cnpg.io/instanceRole=primary -o name | head -1)
  streaming=$(kc -n data exec "$primary" -c postgres -- psql -tAc "select count(*) from pg_stat_replication where state='streaming'")
  [ "$streaming" -eq $((instances - 1)) ] || die "postgres: expected $((instances - 1)) streaming replicas, got $streaming"
  ok "$streaming replicas streaming"

  # the row must show up on a replica (async replication -> retry briefly)
  for _ in 1 2 3 4 5; do
    out=$(run_once apps "$image" psql "$read_url" -tA -c "select pg_is_in_recovery(), count(*) from sdl_smoke where token='$token'") || true
    [ "$out" = "t|1" ] && break
    sleep 2
  done
  [ "$out" = "t|1" ] || die "postgres: replica read via READ_URL failed (got: $out)"
  ok "replica serves the row via READ_URL"
fi
ok "postgres smoke passed (instance $INSTANCE)"
