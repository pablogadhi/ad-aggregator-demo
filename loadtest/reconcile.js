// Reconciliation (spec §10 teardown, §11): k6's accepted clicks per advertiser vs. the analytics
// totals for those advertisers, polled until they all match or TIMEOUT seconds pass.
//
// Runs in-cluster as its own k6-operator TestRun (parallelism 1, see loadtest/run.sh) right after the
// ad-aggregator.js TestRun. The two runs don't share a filesystem, so this reads k6's accepted counts
// back from Prometheus instead of a local result file: ad-aggregator.js counts accepted clicks in a
// Counter tagged `advertiser` (`clicks_accepted_adv`, -> Prometheus series `k6_clicks_accepted_adv_total`)
// and pushes everything via `--out experimental-prometheus-rw --tag testid=<RUN>`; this script sums
// those series by advertiser for the same testid. Host k6 fallback, from the repo root:
//
//   k6 run -e BASE_URL=http://localhost:8080 -e RUN=<id> \
//     -e PROM_URL=http://localhost:8080 -e PROM_HOST=prometheus.localhost loadtest/reconcile.js
//
// Pass criterion (spec §10 teardown, §12): lost = 0 (analytics >= k6 accepted for EVERY advertiser)
// AND over-count (sum of analytics - k6 accepted, where positive) <= ambiguous responses (every
// non-302 answer to a click during the run: 503s + gateway 504s/timeouts/connection errors, read from
// the `clicks_503` / `clicks_other` Prometheus counters for the same testid). Prints the reconciliation
// lag (seconds after the load ended until the totals stopped changing / matched), lost, over-count,
// ambiguous count and pass/fail. Exit code != 0 on fail.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RUN = __ENV.RUN || 'clicks';
const TIMEOUT = Number(__ENV.TIMEOUT || 90);
const PROM = __ENV.PROM_URL || BASE; // in-cluster Prometheus service URL (TestRun) or gateway (host fallback)
const PROM_HOST = __ENV.PROM_HOST || ''; // set to prometheus.localhost for the host fallback via the gateway

const mismatched = new Counter('reconcile_mismatched_advertisers');
const lostMetric = new Counter('reconcile_lost');
const overMetric = new Counter('reconcile_overcount');
const failMetric = new Counter('reconcile_fail');
const lag = new Trend('reconcile_lag_s');

export const options = {
  scenarios: { reconcile: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: `${TIMEOUT + 300}s` } },
  thresholds: { reconcile_fail: ['count==0'] },
};

// ---- Prometheus helpers --------------------------------------------------------------------
function promQuery(q) {
  const headers = PROM_HOST ? { Host: PROM_HOST } : {};
  const r = http.get(`${PROM}/api/v1/query?query=${encodeURIComponent(q)}`, { headers, tags: { kind: 'prom' } });
  if (r.status !== 200) throw new Error(`prometheus query failed: ${r.status} ${r.body}`);
  const body = JSON.parse(r.body);
  if (body.status !== 'success') throw new Error(`prometheus query error: ${r.body}`);
  return body.data.result;
}

function promScalar(q, dflt) {
  const res = promQuery(q);
  return res.length ? Number(res[0].value[1]) : dflt;
}

function promByLabel(q, label) {
  const out = {};
  for (const s of promQuery(q)) out[s.metric[label]] = Number(s.value[1]);
  return out;
}

// ---- k6's accepted counts, from Prometheus -------------------------------------------------
function token(advertiserId) {
  const r = http.post(`${BASE}/api/auth/token`, JSON.stringify({ role: 'advertiser', advertiser_id: Number(advertiserId) }), {
    headers: { 'Content-Type': 'application/json' },
  });
  return r.json('access_token');
}

export default function () {
  const expected = promByLabel(`sum by (advertiser) (k6_clicks_accepted_adv_total{testid="${RUN}"})`, 'advertiser');
  const ids = Object.keys(expected);
  if (ids.length === 0) {
    throw new Error(`reconcile: no k6_clicks_accepted_adv_total series in Prometheus for testid=${RUN} (did the load TestRun finish and push?)`);
  }
  const unavailable503 = promScalar(`sum(k6_clicks_503_total{testid="${RUN}"})`, 0);
  const other = promScalar(`sum(k6_clicks_other_total{testid="${RUN}"})`, 0);
  const ambiguous = unavailable503 + other;
  // last time k6 pushed an accepted-click sample for this run: our approximation of "load ended"
  const ended = promScalar(`max(timestamp(k6_clicks_accepted_adv_total{testid="${RUN}"}))`, Date.now() / 1000) * 1000;
  const from = new Date(ended - 30 * 60 * 1000).toISOString();

  const tokens = {};
  for (const id of ids) tokens[id] = token(id);
  const deadline = Date.now() + TIMEOUT * 1000;
  const got = {};
  let pending = ids.slice();
  let firstSeenAll = null;
  while (true) {
    const to = new Date(Date.now() + 2 * 60 * 1000).toISOString();
    const reqs = pending.map((id) => ({
      method: 'GET',
      url: `${BASE}/api/analytics/advertisers/${id}/clicks?from=${from}&to=${to}&granularity=hour`,
      params: { headers: { Authorization: `Bearer ${tokens[id]}` }, tags: { name: 'reconcile' } },
    }));
    const res = http.batch(reqs);
    pending.forEach((id, i) => {
      if (res[i].status === 200) got[id] = res[i].json('total');
    });
    pending = pending.filter((id) => got[id] !== expected[id]);
    if (pending.length === 0) {
      firstSeenAll = Date.now();
      break;
    }
    if (Date.now() > deadline) break;
    sleep(2);
  }

  // Final tally over every advertiser (matched ones contribute 0/0).
  let lost = 0;
  let over = 0;
  const diffs = [];
  for (const id of ids) {
    const g = got[id] !== undefined ? got[id] : 0;
    const d = g - expected[id];
    if (d < 0) lost += -d;
    else if (d > 0) over += d;
    if (d !== 0) diffs.push([id, expected[id], g, d]);
  }
  const pass = lost === 0 && over <= ambiguous;

  const expTotal = ids.reduce((s, id) => s + expected[id], 0);
  const gotTotal = ids.reduce((s, id) => s + (got[id] || 0), 0);
  console.log(`reconcile run=${RUN}: k6 accepted=${expTotal} (${ids.length} advertisers, from Prometheus), analytics=${gotTotal}`);
  console.log(`reconcile: ambiguous=${ambiguous} (503=${unavailable503} other=${other})`);

  if (diffs.length === 0) {
    firstSeenAll = firstSeenAll || Date.now();
    const l = (firstSeenAll - ended) / 1000;
    lag.add(l);
    console.log(`reconcile: EXACT MATCH for ${ids.length} advertisers, ${l.toFixed(1)}s after the load ended`);
  } else {
    const l = (Date.now() - ended) / 1000;
    lag.add(l);
    for (const [id, exp, g, d] of diffs) {
      console.log(`  advertiser ${id}: k6 accepted=${exp} analytics=${g} diff=${d > 0 ? '+' : ''}${d}`);
    }
    mismatched.add(diffs.length);
    console.log(`reconcile: ${diffs.length} advertisers differ after ${l.toFixed(1)}s (timeout=${TIMEOUT}s)`);
  }

  lostMetric.add(lost);
  overMetric.add(over);
  if (!pass) failMetric.add(1);
  console.log(`reconcile: lost=${lost} over-count=${over} ambiguous=${ambiguous} -> ${pass ? 'PASS' : 'FAIL'} (criterion: lost==0 and over-count<=ambiguous)`);
}
