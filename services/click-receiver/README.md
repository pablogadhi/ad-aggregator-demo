# click-receiver

The hot path (spec §5.1, §5.1.1; contract `design/contracts/openapi/click-receiver.yaml`).

```
GET /click/{ad_id}?user_id=… | POST /clicks
  ad lookup: in-process cache (60 s, coalesced misses) → POSTGRES_READ_URL → POSTGRES_URL on a miss
  dedup:     SET click:dedup:<ad>:<user> <click_id> NX EX 600   (50 ms; error/timeout → fail open)
  produce:   clicks, key <ad> or <ad>#<salt> (hot), acks=all + idempotence, WAIT for the ack
             failure/timeout (≤ 1.5 s) → DEL dedup key → 503 (never "accepted" without an ack)
GET /hot-ads   this receiver's cached hot set
```

- One Redis round trip per click (the SET NX). Hot-ad counting/marking is batched every 1 s and
  the hot set is refreshed every 2 s in the background (`hot.py`, `store.py`); if Redis is down the
  last known hot set is kept (hot ads stay salted).
- Kafka: confluent-kafka (librdkafka) with delivery reports bridged to asyncio futures
  (`sdl_common.kafka.AsyncProducer`). Redis: redis-py asyncio, `RedisCluster` when `REDIS_MODE=cluster`.
- Readiness: Kafka metadata for `clicks` + *any* ads-DB pool (replica or primary) answers; Redis
  is not a readiness dependency (fail open).
- Metrics: `clicks_total{status,hot}`, `click_dedup_failopen_total`, `click_produce_seconds`,
  `hot_ads`, `hot_ads_permanent`, `hot_ad_markings_total`, plus flush/refresh error counters.

**Resetting a permanently hot ad** (manual, spec §5.1.1):
`redis-cli -c SREM hot:perm <ad_id>` and `redis-cli -c DEL 'hot:{a:<ad_id>}:marks'`.
