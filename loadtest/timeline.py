#!/usr/bin/env python3
"""Per-bucket click timeline from a k6 CSV output (chaos runs, spec §11).

    k6 run ... --out csv=loadtest/results/<run>.csv loadtest/ad-aggregator.js
    python3 loadtest/timeline.py loadtest/results/<run>.csv [bucket_seconds=10] [request_name] [ok_status]
    e.g. ... 5 "POST ad" 201   (ad-placement write probe, WRITES=1)

Prints, per bucket: requests, non-302 count (by status), error %, p50/p95/max latency. Then the
recovery summary: first/last bucket with errors or p95 >= 100 ms. Stdlib only.
"""

import csv
import gzip
import sys
from collections import Counter, defaultdict


def pct(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    k = min(len(values) - 1, max(0, round(p / 100 * (len(values) - 1))))
    return values[k]


def main(path: str, bucket: int = 10, name: str = "GET /click/{ad_id}", ok: str = "302") -> None:
    opener = gzip.open if path.endswith(".gz") else open
    lat = defaultdict(list)
    codes = defaultdict(Counter)
    t0 = None
    with opener(path, "rt", newline="") as f:
        for row in csv.DictReader(f):
            if row["metric_name"] != "http_req_duration" or row.get("name") != name:
                continue
            ts = int(float(row["timestamp"]))
            t0 = ts if t0 is None else min(t0, ts)
            b = ts // bucket * bucket
            lat[b].append(float(row["metric_value"]))
            codes[b][row["status"]] += 1
    if not lat:
        sys.exit("no click samples in " + path)
    print(f"{'t(s)':>6} {'reqs':>6} {'err%':>7} {'p50':>7} {'p95':>7} {'max':>8}  non-302")
    bad = []
    for b in sorted(lat):
        n = len(lat[b])
        errs = {k: v for k, v in codes[b].items() if k != ok}
        e = sum(errs.values())
        p95 = pct(lat[b], 95)
        print(f"{b - t0:>6} {n:>6} {100 * e / n:>6.2f}% {pct(lat[b], 50):>7.1f} {p95:>7.1f} {max(lat[b]):>8.1f}  {errs or ''}")
        if e or p95 >= 100:
            bad.append(b - t0)
    total = sum(len(v) for v in lat.values())
    total_err = sum(v for c in codes.values() for k, v in c.items() if k != ok)
    print(f"\ntotal {total} clicks, {total_err} non-302 ({100 * total_err / total:.3f}%)")
    if bad:
        print(f"degraded buckets (errors or p95>=100ms): first at t={bad[0]}s, last at t={bad[-1]}s, "
              f"{len(bad)} buckets -> degraded window ~{bad[-1] - bad[0] + bucket}s")
    else:
        print("no degraded buckets")


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 10, *sys.argv[3:5])
