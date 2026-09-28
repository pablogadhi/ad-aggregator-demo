// Load test for ad-aggregator (design/spec.md §10). In-cluster (primary): `make load S=clicks`
// (see loadtest/run.sh) runs this as a k6-operator TestRun, 3 runners spread across zones, results
// pushed to Prometheus remote write tagged `testid=<RUN>`. Host k6 fallback, from the repo root:
//
//   k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=clicks -e RUN=<id> \
//     -e K6_PROMETHEUS_RW_SERVER_URL=http://localhost:8080/api/v1/write \
//     -e PROM_URL=http://localhost:8080 --out experimental-prometheus-rw --tag testid=<id> loadtest/ad-aggregator.js
//   RUN_STARTED_AT=<iso> RUN_ENDED_AT=<iso> k6 run -e BASE_URL=http://localhost:8080 -e RUN=<id> loadtest/reconcile.js
//
// (host reconcile needs a Host: prometheus.localhost header on PROM_URL requests; see loadtest/README.md)
//
// SCENARIO (default clicks):
//   clicks  500 rps x 5 min, then 1,000 rps x 1 min (uniform ads) + 20 rps analytics queries
//   hot     same shape, 20 % of the clicks on one ad (run with HOT_SALTING_ENABLED=true and false)
//   scale   ramp 200 -> 2,000 rps over 10 min, hold 5 min (thresholds are reported, not gates)
//   chaos   300 rps x 5 min (spec §11); HOT=1 adds the 20 % hot ad (chaos #9). DURATION/RATE override.
//
// Setup creates 100 advertisers x 20 ads through the API (fresh every run). Every VU counts
// `X-Click-Status: accepted` in a Counter tagged `advertiser` (the real advertiser id) so
// reconcile.js can read per-advertiser accepted counts back from Prometheus (no shared result file
// across the parallel runner pods).
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const SCENARIO = __ENV.SCENARIO || 'clicks';
const RUN = __ENV.RUN || SCENARIO;
const N_ADV = 100;
const ADS_PER_ADV = 20;
const USER_POOL = 1000000;
const HOT = SCENARIO === 'hot' || __ENV.HOT === '1';
const HOT_SHARE = 0.2;
const PROM = __ENV.PROM_URL || BASE; // Prometheus: in-cluster service URL (TestRun) or gateway w/ Host header (host fallback)

// ---- metrics --------------------------------------------------------------------------------
const acceptedByAdv = new Counter('clicks_accepted_adv'); // tagged { advertiser: <id> }
const accepted = new Counter('clicks_accepted');
const duplicate = new Counter('clicks_duplicate');
const unavailable = new Counter('clicks_503');
const otherStatus = new Counter('clicks_other');
const click302 = new Rate('click_302');
const hotSeenAt = new Trend('hot_seen_at_s'); // seconds after load start when the hot ad answered X-Click-Hot: true
const hotAcceptedHot = new Rate('hot_ad_clicks_hot'); // share of the hot ad's clicks answered as hot
const partitionSkew = new Trend('partition_skew'); // max/min per-partition message rate of `clicks`

// ---- scenarios --------------------------------------------------------------------------------
function clickScenarios() {
  const base = { exec: 'click', preAllocatedVUs: 200, maxVUs: 2000, timeUnit: '1s' };
  if (SCENARIO === 'scale') {
    return {
      scale: {
        ...base, executor: 'ramping-arrival-rate', startRate: 200, preAllocatedVUs: 500, maxVUs: 4000,
        stages: [{ target: 2000, duration: '10m' }, { target: 2000, duration: '5m' }],
      },
    };
  }
  if (SCENARIO === 'chaos') {
    const s = {
      chaos: { ...base, executor: 'constant-arrival-rate', rate: Number(__ENV.RATE || 300), duration: __ENV.DURATION || '5m' },
    };
    // WRITES=1: ad-placement write probe (chaos #5: ads-DB failover), 5 creates/s
    if (__ENV.WRITES === '1') {
      s.writes = { executor: 'constant-arrival-rate', exec: 'write', rate: 5, timeUnit: '1s', duration: __ENV.DURATION || '5m', preAllocatedVUs: 10, maxVUs: 100 };
    }
    // QUERIES=1: 20 rps analytics queries alongside (chaos #6: analytics-db outage)
    if (__ENV.QUERIES === '1') {
      s.queries = { executor: 'constant-arrival-rate', exec: 'query', rate: 20, timeUnit: '1s', duration: __ENV.DURATION || '5m', preAllocatedVUs: 20, maxVUs: 200 };
    }
    return s;
  }
  // clicks / hot
  return {
    clicks: { ...base, executor: 'constant-arrival-rate', rate: 500, duration: '5m' },
    clicks_burst: { ...base, executor: 'constant-arrival-rate', rate: 1000, duration: '1m', startTime: '5m' },
    queries: { executor: 'constant-arrival-rate', exec: 'query', rate: 20, timeUnit: '1s', duration: '6m', preAllocatedVUs: 20, maxVUs: 200 },
  };
}

function thresholds() {
  if (SCENARIO === 'scale') {
    // findings, not gates: tracked so they show up in the summary (spec §10)
    return { 'http_req_duration{kind:click}': ['p(95)<100000'], clicks_accepted: ['count>0'] };
  }
  if (SCENARIO === 'chaos') {
    return {
      'http_req_failed{kind:click}': ['rate<0.01'], // chaos: click errors < 1 %
      click_302: ['rate>0.99'],
      'http_req_duration{kind:click}': ['p(95)<100000'], // reported (timeline via --out csv)
    };
  }
  const t = {
    'http_req_duration{scenario:clicks}': ['p(95)<100'],
    'http_req_duration{scenario:clicks_burst}': ['p(95)<100'],
    'http_req_failed{kind:click}': ['rate<0.001'],
    click_302: ['rate>0.999'],
    'http_req_duration{scenario:queries}': ['p(95)<500'],
    'http_req_failed{scenario:queries}': ['rate<0.001'],
  };
  if (HOT) {
    t.hot_seen_at_s = ['min<30'];
    if ((__ENV.EXPECT_SALTING || 'true') === 'true') t.partition_skew = ['max<2'];
  }
  return t;
}

export const options = {
  scenarios: clickScenarios(),
  thresholds: thresholds(),
  setupTimeout: '5m',
  teardownTimeout: '2m',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: true,
};

// ---- setup: 100 advertisers x 20 ads ------------------------------------------------------------
function post(path, body, token) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  return http.post(`${BASE}${path}`, JSON.stringify(body), { headers, responseType: 'text', tags: { kind: 'setup' } });
}

function tokenFor(body) {
  const r = post('/api/auth/token', body);
  if (r.status !== 200) throw new Error(`token ${r.status} ${r.body}`);
  return r.json('access_token');
}

export function setup() {
  const runId = `${RUN}-${Date.now()}`;
  const viewer = tokenFor({ role: 'viewer', user_id: `load-${runId}` });
  const advertisers = [];
  const ads = []; // [ad_id, advertiser_index]
  for (let i = 0; i < N_ADV; i++) {
    const r = post('/api/ad-placement/advertisers', { name: `load ${runId} #${i}` }, viewer);
    if (r.status !== 201) throw new Error(`advertiser ${r.status} ${r.body}`);
    const id = r.json('id');
    advertisers.push(id);
    const tok = tokenFor({ role: 'advertiser', advertiser_id: id });
    const reqs = [];
    for (let j = 0; j < ADS_PER_ADV; j++) {
      reqs.push({
        method: 'POST',
        url: `${BASE}/api/ad-placement/advertisers/${id}/ads`,
        body: JSON.stringify({ content: `ad ${j} of ${id}`, redirect_url: `http://localhost:8080/landing/${id}-${j}` }),
        params: { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${tok}` }, responseType: 'text', tags: { kind: 'setup' } },
      });
    }
    for (const res of http.batch(reqs)) {
      if (res.status !== 201) throw new Error(`ad ${res.status} ${res.body}`);
      ads.push([res.json('id'), i]);
    }
  }
  // one advertiser token per query VU iteration is cheap enough to precompute for the queries scenario
  const queryTokens = advertisers.slice(0, 10).map((id) => tokenFor({ role: 'advertiser', advertiser_id: id }));
  const hotIndex = Math.floor(Math.random() * ads.length);
  const t0 = Date.now();
  console.log(`setup: run=${runId} advertisers=${advertisers[0]}..${advertisers[N_ADV - 1]} ads=${ads.length} hot_ad=${HOT ? ads[hotIndex][0] : '-'}`);
  return { runId, advertisers, ads, hotIndex, queryTokens, t0, startedAt: new Date(t0).toISOString() };
}

// ---- clicks ----------------------------------------------------------------------------------
export function click(data) {
  const isHot = HOT && Math.random() < HOT_SHARE;
  const [adId, advIdx] = data.ads[isHot ? data.hotIndex : Math.floor(Math.random() * data.ads.length)];
  const user = `lu-${Math.floor(Math.random() * USER_POOL)}`;
  const res = http.get(`${BASE}/api/click-receiver/click/${adId}?user_id=${user}`, {
    redirects: 0,
    timeout: '5s',
    tags: { kind: 'click', name: 'GET /click/{ad_id}' },
  });
  const ok = res.status === 302;
  click302.add(ok);
  if (ok) {
    const status = res.headers['X-Click-Status'];
    if (status === 'accepted') {
      accepted.add(1);
      acceptedByAdv.add(1, { advertiser: String(data.advertisers[advIdx]) });
    } else if (status === 'duplicate') {
      duplicate.add(1);
    } else {
      otherStatus.add(1);
    }
    if (isHot) {
      const hot = res.headers['X-Click-Hot'] === 'true';
      hotAcceptedHot.add(hot);
      if (hot) hotSeenAt.add((Date.now() - data.t0) / 1000);
    }
  } else if (res.status === 503) {
    unavailable.add(1);
  } else {
    otherStatus.add(1);
  }
  check(res, { 'click 302': () => ok });
}

// ---- ad-placement write probe (WRITES=1) ------------------------------------------------------
const writeOk = new Rate('ad_write_ok');
export function write(data) {
  const adv = data.advertisers[0];
  const res = http.post(`${BASE}/api/ad-placement/advertisers/${adv}/ads`,
    JSON.stringify({ content: 'probe', redirect_url: 'http://localhost:8080/landing/probe' }), {
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${data.queryTokens[0]}` },
      timeout: '20s',
      tags: { kind: 'write', name: 'POST ad' },
    });
  writeOk.add(res.status === 201);
}

// ---- analytics queries (20 rps) ----------------------------------------------------------------
export function query(data) {
  const i = Math.floor(Math.random() * data.queryTokens.length);
  const adv = data.advertisers[i];
  const headers = { Authorization: `Bearer ${data.queryTokens[i]}` };
  const to = new Date();
  const from = new Date(to.getTime() - 3600 * 1000);
  const qs = `from=${from.toISOString()}&to=${to.toISOString()}&granularity=minute`;
  let res;
  if (exec.scenario.iterationInTest % 2 === 0) {
    res = http.get(`${BASE}/api/analytics/advertisers/${adv}/clicks?${qs}`, { headers, tags: { kind: 'query', name: 'advertiser clicks' } });
  } else {
    const ad = data.ads.find((a) => a[1] === i)[0];
    res = http.get(`${BASE}/api/analytics/advertisers/${adv}/ads/${ad}/clicks?${qs}`, { headers, tags: { kind: 'query', name: 'ad clicks' } });
  }
  check(res, { 'query 200': (r) => r.status === 200 });
}

// ---- teardown: per-partition message rates of `clicks` (hot-ad scenario) ------------------------
function prom(q) {
  const r = http.get(`${PROM}/api/v1/query?query=${encodeURIComponent(q)}`, {
    headers: { Host: 'prometheus.localhost' }, responseType: 'text', tags: { kind: 'prom' },
  });
  if (r.status !== 200) return null;
  return JSON.parse(r.body).data.result;
}

export function teardown(data) {
  if (!HOT) return;
  const secs = Math.floor((Date.now() - data.t0) / 1000);
  const res = prom(`sum by (partition) (increase(kafka_topic_partition_current_offset{topic="clicks"}[${secs}s]))`);
  if (!res || res.length === 0) {
    console.warn('teardown: no kafka exporter data in Prometheus');
    return;
  }
  const rates = res.map((s) => [Number(s.metric.partition), Number(s.value[1]) / secs]).sort((a, b) => a[0] - b[0]);
  const vals = rates.map((r) => r[1]);
  const skew = Math.max(...vals) / Math.max(Math.min(...vals), 1e-9);
  partitionSkew.add(skew);
  console.log(`teardown: per-partition msg/s over ${secs}s: ${rates.map((r) => `p${r[0]}=${r[1].toFixed(1)}`).join(' ')}  max/min=${skew.toFixed(2)}`);
}

// ---- summary: counts for reconcile.js -----------------------------------------------------------
function metricVal(data, name, stat) {
  const m = data.metrics[name];
  return m ? m.values[stat] : undefined;
}

export function handleSummary(data) {
  const setup = data.setup_data || {};
  const out = {
    run: setup.runId,
    scenario: SCENARIO,
    hot: HOT,
    hot_ad: HOT && setup.ads ? setup.ads[setup.hotIndex][0] : null,
    started_at: setup.startedAt,
    ended_at: new Date().toISOString(),
    accepted_total: metricVal(data, 'clicks_accepted', 'count') || 0,
    duplicate_total: metricVal(data, 'clicks_duplicate', 'count') || 0,
    unavailable_503: metricVal(data, 'clicks_503', 'count') || 0,
    other: metricVal(data, 'clicks_other', 'count') || 0,
  };
  const lines = [];
  lines.push(`\n=== ad-aggregator load: scenario=${SCENARIO} run=${out.run} testid=${RUN} ===`);
  for (const [name, m] of Object.entries(data.metrics).sort()) {
    const v = m.values;
    const th = m.thresholds ? Object.entries(m.thresholds).map(([k, r]) => `${k}:${r.ok ? 'ok' : 'FAIL'}`).join(' ') : '';
    let s;
    if (m.type === 'trend') s = `avg=${v.avg.toFixed(1)} p95=${(v['p(95)'] || 0).toFixed(1)} p99=${(v['p(99)'] || 0).toFixed(1)} max=${v.max.toFixed(1)} min=${v.min.toFixed(1)}`;
    else if (m.type === 'rate') s = `rate=${(v.rate * 100).toFixed(3)}% (${v.passes}/${v.passes + v.fails})`;
    else if (m.type === 'counter') s = `count=${v.count} rate=${v.rate.toFixed(1)}/s`;
    else s = JSON.stringify(v);
    if (th || /^(http_req_duration|http_req_failed|http_reqs|iterations|dropped_iterations|clicks_|click_302|hot_|partition_skew|checks|ad_write_ok)/.test(name)) {
      lines.push(`${name.padEnd(55)} ${s} ${th}`);
    }
  }
  lines.push(`accepted=${out.accepted_total} duplicate=${out.duplicate_total} 503=${out.unavailable_503} other=${out.other}`);
  lines.push(`per-advertiser accepted counts + ambiguous (503+other) are in Prometheus, tagged testid=${RUN}; run reconcile.js next\n`);
  return { stdout: lines.join('\n') };
}
