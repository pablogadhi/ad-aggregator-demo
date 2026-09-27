# Load tests (design/spec.md §10)

```bash
k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=clicks -e RUN=clicks loadtest/ad-aggregator.js
k6 run -e BASE_URL=http://localhost:8080 -e RUN=clicks loadtest/reconcile.js
```

- `ad-aggregator.js`: `SCENARIO=clicks` (500 rps × 5 min + 1,000 rps × 1 min, + 20 rps queries), `hot`
  (20 % on one ad; `EXPECT_SALTING=false` when the receivers run with `HOT_SALTING_ENABLED=false`),
  `scale` (200 → 2,000 rps over 10 min, hold 5 min), `chaos` (300 rps × 5 min, `HOT=1`, `RATE`, `DURATION`).
  Setup creates 100 advertisers × 20 ads. Accepted clicks are counted per advertiser and written to
  `loadtest/results/<RUN>.json` by `handleSummary`.
- `reconcile.js`: polls the analytics totals of those advertisers until they equal k6's accepted counts
  (or `TIMEOUT`, default 90 s); prints the lag, over-count and loss; fails on mismatch.
- `timeline.py`: error % and p95 per 10 s from `--out csv=loadtest/results/<RUN>.csv` (chaos runs).
- The hot scenario's teardown reads per-partition message rates of `clicks` from Prometheus
  (kafka-exporter `kafka_topic_partition_current_offset`) and gates `max/min < 2` with salting.
