// Reconciliation (spec §10 teardown, §11): k6's accepted clicks per advertiser (written by
// ad-aggregator.js to loadtest/results/<RUN>.json) vs. the analytics totals for those advertisers,
// polled until they all match or TIMEOUT seconds pass.
//
//   k6 run -e BASE_URL=http://localhost:8080 -e RUN=<name> [-e TIMEOUT=90] loadtest/reconcile.js
//
// Prints the reconciliation lag (seconds after the load ended until every advertiser matched) and,
// when they don't match, every difference (over-count > 0, loss < 0). Exit code != 0 on mismatch.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RUN = __ENV.RUN || 'clicks';
const TIMEOUT = Number(__ENV.TIMEOUT || 90);
const result = JSON.parse(open(__ENV.RESULT || `./results/${RUN}.json`));

const mismatched = new Counter('reconcile_mismatched_advertisers');
const lost = new Counter('reconcile_lost');
const over = new Counter('reconcile_overcount');
const lag = new Trend('reconcile_lag_s');

export const options = {
  scenarios: { reconcile: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: `${TIMEOUT + 300}s` } },
  thresholds: { reconcile_mismatched_advertisers: ['count==0'] },
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
  const expTotal = ids.reduce((s, id) => s + expected[id], 0);
  const gotTotal = ids.reduce((s, id) => s + (got[id] || 0), 0);
  console.log(`reconcile run=${result.run} scenario=${result.scenario}: k6 accepted=${expTotal} (result file: ${result.accepted_total}), analytics=${gotTotal}, 503s during load=${result.unavailable_503}`);
  if (firstSeenAll) {
    const l = (firstSeenAll - ended) / 1000;
    lag.add(l);
    console.log(`reconcile: EXACT MATCH for ${ids.length} advertisers, ${l.toFixed(1)}s after the load ended`);
  } else {
    let o = 0;
    let lo = 0;
    for (const id of pending) {
      const d = (got[id] || 0) - expected[id];
      if (d > 0) o += d;
      else lo += -d;
      console.log(`  advertiser ${id}: k6 accepted=${expected[id]} analytics=${got[id]} diff=${d > 0 ? '+' : ''}${d}`);
    }
    mismatched.add(pending.length);
    over.add(o);
    lost.add(lo);
    console.log(`reconcile: MISMATCH after ${TIMEOUT}s: ${pending.length} advertisers, over-count=${o}, lost=${lo}`);
  }
}
