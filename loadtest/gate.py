#!/usr/bin/env python3
"""Aggregated Prometheus gates for an in-cluster k6-operator load TestRun (design/spec.md §10).

k6-operator evaluates thresholds per runner pod; with parallelism > 1 that's not the real picture.
This queries the aggregated series for the run's `testid` tag (summed/maxed across the parallel
runners) and applies the same thresholds ad-aggregator.js declares in `thresholds()`, so `make load`
gates on the whole run, not one runner. Percentiles are reported as a max-of-per-runner-percentile
(k6's Prometheus remote-write output has no cross-runner percentile merge without native histograms,
which this lab's Prometheus isn't configured for) -- a conservative, not exact, aggregate; noted in
the output. Error/redirect rates are computed from additive Counters (exact).

    python3 loadtest/gate.py <scenario> <run> [prom_url] [prom_host]

Exit code != 0 if any hard gate fails (scenario in clicks/hot/chaos). `scale` only reports.
"""
import json
import sys
import urllib.parse
import urllib.request


def prom_query(prom_url: str, prom_host: str, q: str):
    url = f"{prom_url}/api/v1/query?query={urllib.parse.quote(q)}"
    req = urllib.request.Request(url, headers={"Host": prom_host} if prom_host else {})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = json.load(r)
    if body["status"] != "success":
        raise RuntimeError(f"prometheus query failed: {q} -> {body}")
    return body["data"]["result"]


def scalar(prom_url, prom_host, q, default=0.0):
    res = prom_query(prom_url, prom_host, q)
    return float(res[0]["value"][1]) if res else default


def max_p95_ms(prom_url, prom_host, testid, extra=""):
    v = scalar(prom_url, prom_host, f'max(k6_http_req_duration_p95{{testid="{testid}"{extra}}})', None)
    return None if v is None else v * 1000


def click_error_rate(prom_url, prom_host, testid):
    total = scalar(prom_url, prom_host, f'sum(k6_http_reqs_total{{testid="{testid}",kind="click"}})', 0)
    non302 = scalar(prom_url, prom_host, f'sum(k6_clicks_503_total{{testid="{testid}"}})', 0) + \
        scalar(prom_url, prom_host, f'sum(k6_clicks_other_total{{testid="{testid}"}})', 0)
    return (non302 / total if total else 0.0), total, non302


def main():
    scenario = sys.argv[1]
    run = sys.argv[2]
    prom_url = sys.argv[3] if len(sys.argv) > 3 else "http://localhost:8080"
    prom_host = sys.argv[4] if len(sys.argv) > 4 else "prometheus.localhost"

    print(f"\n=== aggregated Prometheus gates: scenario={scenario} testid={run} ===")
    ok = True

    def gate(label, value, unit, cond, cond_desc):
        nonlocal ok
        passed = cond(value) if value is not None else False
        ok = ok and passed
        print(f"  {label:<28} {value if value is not None else 'n/a':>10}{unit}  {'ok' if passed else 'FAIL'} ({cond_desc})")

    def report(label, value, unit):
        print(f"  {label:<28} {value if value is not None else 'n/a':>10}{unit}  (reported)")

    if scenario in ("clicks", "hot"):
        p95_clicks = max_p95_ms(prom_url, prom_host, run, ',scenario="clicks"')
        p95_burst = max_p95_ms(prom_url, prom_host, run, ',scenario="clicks_burst"')
        p95_queries = max_p95_ms(prom_url, prom_host, run, ',kind="query",scenario="queries"')
        err, total, non302 = click_error_rate(prom_url, prom_host, run)
        r302 = 1 - err
        gate("click p95 (steady, max/runner)", round(p95_clicks, 1) if p95_clicks is not None else None, "ms", lambda v: v < 100, "<100ms")
        gate("click p95 (burst, max/runner)", round(p95_burst, 1) if p95_burst is not None else None, "ms", lambda v: v < 100, "<100ms")
        gate("click error rate", round(err * 100, 4), "%", lambda v: v < 0.1, "<0.1%")
        gate("click 302 rate", round(r302 * 100, 4), "%", lambda v: v > 99.9, ">99.9%")
        gate("query p95 (max/runner)", round(p95_queries, 1) if p95_queries is not None else None, "ms", lambda v: v < 500, "<500ms")
        print(f"  click requests={int(total)} non-302={int(non302)}")
        if scenario == "hot":
            hot_min = scalar(prom_url, prom_host, f'min(k6_hot_seen_at_s_min{{testid="{run}"}})', None)
            gate("hot ad seen at (min/runner)", round(hot_min, 1) if hot_min is not None else None, "s", lambda v: v < 30, "<30s")
            skew = scalar(prom_url, prom_host, f'max(k6_partition_skew_max{{testid="{run}"}})', None)
            report("partition skew max/min (max/runner)", round(skew, 2) if skew is not None else None, "")
    elif scenario == "chaos":
        p95 = max_p95_ms(prom_url, prom_host, run, ',kind="click"')
        err, total, non302 = click_error_rate(prom_url, prom_host, run)
        r302 = 1 - err
        report("click p95 (max/runner)", round(p95, 1) if p95 is not None else None, "ms")
        gate("click error rate", round(err * 100, 4), "%", lambda v: v < 1.0, "<1%")
        gate("click 302 rate", round(r302 * 100, 4), "%", lambda v: v > 99.0, ">99%")
        print(f"  click requests={int(total)} non-302={int(non302)}")
    else:  # scale: findings, not gates
        p95 = max_p95_ms(prom_url, prom_host, run, ',kind="click"')
        err, total, non302 = click_error_rate(prom_url, prom_host, run)
        report("click p95 (max/runner)", round(p95, 1) if p95 is not None else None, "ms")
        report("click error rate", round(err * 100, 4), "%")
        print(f"  click requests={int(total)} non-302={int(non302)}")

    print(f"=== gates: {'PASS' if ok else 'FAIL'} ===\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
