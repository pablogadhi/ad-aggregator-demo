// Reconciliation (spec §10 teardown, §11): k6's accepted clicks per advertiser (written by
// ad-aggregator.js to loadtest/results/<RUN>.json) vs. the analytics totals for those advertisers,
// polled until they all match or TIMEOUT seconds pass.
//
//   k6 run -e BASE_URL=http://localhost:8080 -e RUN=<name> [-e TIMEOUT=90] loadtest/reconcile.js
//
// Pass criterion (spec §10 teardown, §12): lost = 0 (analytics >= k6 accepted for EVERY advertiser)
// AND over-count (sum of analytics - k6 accepted, where positive) <= ambiguous responses (every
// non-302 answer to a click during the run: 503s + gateway 504s/timeouts/connection errors, i.e.
// result.unavailable_503 + result.other from the load run's summary). Prints the reconciliation lag
// (seconds after the load ended until the totals stopped changing / matched), lost, over-count,
// ambiguous count and pass/fail. Exit code != 0 on fail.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RUN = __ENV.RUN || 'clicks';
const TIMEOUT = Number(__ENV.TIMEOUT || 90);
const result = JSON.parse(open(__ENV.RESULT || `./results/${RUN}.json`));

const mismatched = new Counter('reconcile_mismatched_advertisers');
const lostMetric = new Counter('reconcile_lost');
const overMetric = new Counter('reconcile_overcount');
const failMetric = new Counter('reconcile_fail');
const lag = new Trend('reconcile_lag_s');

export const options = {
  scenarios: { reconcile: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: `${TIMEOUT + 300}s` } },
  thresholds: { reconcile_fail: ['count==0'] },
};

function token(advertiserId) {
  const r = http.post(`${BASE}/api/auth/token`, JSON.stringify({ role: 'advertiser', advertiser_id: Number(advertiserId) }), {
    headers: { 'Content-Type': 'application/json' },
  });
  return r.json('access_token');
}

export default function () {
  const expected = result.accepted_by_advertiser;
  const ids = Object.keys(expected);
  const tokens = {};
  for (const id of ids) tokens[id] = token(id);
  const from = new Date(Date.parse(result.started_at) - 5 * 60 * 1000).toISOString();
  const ended = Date.parse(result.ended_at);
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
  const ambiguous = (result.unavailable_503 || 0) + (result.other || 0);
  const pass = lost === 0 && over <= ambiguous;

  const expTotal = ids.reduce((s, id) => s + expected[id], 0);
  const gotTotal = ids.reduce((s, id) => s + (got[id] || 0), 0);
  console.log(`reconcile run=${result.run} scenario=${result.scenario}: k6 accepted=${expTotal} (result file: ${result.accepted_total}), analytics=${gotTotal}`);
  console.log(`reconcile: ambiguous=${ambiguous} (503=${result.unavailable_503 || 0} other=${result.other || 0})`);

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
