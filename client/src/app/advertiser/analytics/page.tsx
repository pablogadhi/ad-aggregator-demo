"use client";
// Advertiser analytics. Flows: §8.5 aggregation ≤ 60 s (auto-refresh + freshness), §8.6 rollups (granularity
// switch; > 1,440 buckets → 400), §8.1 auth at the edge (no token / tampered token → 401, other advertiser → 403).
import { useCallback, useEffect, useState } from "react";
import type { components } from "@/api/analytics";
import { api, type Served } from "@/lib/api";
import { advertiserSession, storedAdvertiser } from "@/lib/session";
import { ErrorBox, ServedBy, ago, usePolling, useNow } from "@/lib/ui";
import { BarChart } from "./BarChart";

type S = components["schemas"];
type Granularity = S["AdvertiserClicks"]["granularity"];
type Result = S["AdvertiserClicks"] | S["AdClicks"];
type TokenMode = "mine" | "none" | "tampered";

const RANGES = [
  { label: "60 min", ms: 60 * 60_000 },
  { label: "6 h", ms: 6 * 3600_000 },
  { label: "24 h", ms: 24 * 3600_000 },
  { label: "7 days", ms: 7 * 24 * 3600_000 },
];
const GRANULARITIES: Granularity[] = ["minute", "hour", "day"];
const REFRESH_MS = 5000;

/** Flip one character in the signature so the gateway's JWT check fails (401). */
function tamper(token: string): string {
  const [h, p, sig = ""] = token.split(".");
  const i = Math.floor(sig.length / 2);
  return `${h}.${p}.${sig.slice(0, i)}${sig[i] === "A" ? "B" : "A"}${sig.slice(i + 1)}`;
}

export default function AnalyticsPage() {
  const [ownId, setOwnId] = useState<number | null>(null);
  const [ready, setReady] = useState(false);
  const [pathId, setPathId] = useState("");
  const [adId, setAdId] = useState<number | null>(null);
  const [range, setRange] = useState(0);
  const [granularity, setGranularity] = useState<Granularity>("minute");
  const [tokenMode, setTokenMode] = useState<TokenMode>("mine");
  const [auto, setAuto] = useState(true);
  const [res, setRes] = useState<Served<Result> | null>(null);
  const [fetchedAt, setFetchedAt] = useState<number | null>(null);
  const [error, setError] = useState<unknown>(null);
  const now = useNow();

  useEffect(() => {
    const s = storedAdvertiser();
    setOwnId(s?.advertiserId ?? null);
    setPathId(s ? String(s.advertiserId) : "");
    setReady(true);
  }, []);

  const advertiserId = Number(pathId);
  const valid = advertiserId > 0;

  const load = useCallback(async () => {
    if (!valid) return;
    try {
      const s = tokenMode === "none" ? null : await advertiserSession();
      const token = tokenMode === "none" ? null : tokenMode === "tampered" && s ? tamper(s.token) : s?.token;
      const from = new Date(Date.now() - RANGES[range].ms).toISOString();
      const q = `?granularity=${granularity}&from=${encodeURIComponent(from)}`;
      const path = adId ? `/advertisers/${advertiserId}/ads/${adId}/clicks${q}` : `/advertisers/${advertiserId}/clicks${q}`;
      const r = await api<Result>("analytics", path, { token });
      setRes(r);
      setError(null);
    } catch (e) {
      setError(e);
    } finally {
      setFetchedAt(Date.now());
    }
  }, [valid, advertiserId, adId, range, granularity, tokenMode]);

  // Reload immediately when a control changes; then every 5 s while auto-refresh is on.
  useEffect(() => {
    load();
  }, [load]);
  usePolling(load, REFRESH_MS, auto && valid, false);

  if (!ready) return null;

  const data = res?.data;
  const byAd = data && "by_ad" in data ? data.by_ad : null;
  const bucketLabel = (iso: string) => {
    const d = new Date(iso);
    return granularity === "day"
      ? d.toLocaleDateString()
      : granularity === "hour" && RANGES[range].ms > 24 * 3600_000
        ? `${d.toLocaleDateString()} ${d.getHours()}:00`
        : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  };
  const lastUpdated = data?.freshness.last_updated_at ?? null;

  return (
    <>
      <h1>Analytics</h1>
      {ownId === null ? (
        <p className="alert">
          Not logged in as an advertiser: <a href="/advertiser">log in</a> first (or try the path below with no token to
          see the gateway&apos;s 401).
        </p>
      ) : (
        <p className="muted">
          Token for advertiser <strong>#{ownId}</strong>. Counts are aggregated by Flink per minute and upserted into
          the analytics DB: a click shows up here within ~60 s.
        </p>
      )}

      <div className="controls">
        <label>
          Range{" "}
          <select value={range} onChange={(e) => setRange(Number(e.target.value))}>
            {RANGES.map((r, i) => (
              <option key={r.label} value={i}>
                {r.label}
              </option>
            ))}
          </select>
        </label>
        <div className="seg" role="group" aria-label="Granularity">
          {GRANULARITIES.map((g) => (
            <button key={g} className={g === granularity ? "" : "secondary"} onClick={() => setGranularity(g)} aria-pressed={g === granularity}>
              {g}
            </button>
          ))}
        </div>
        <label>
          <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} /> auto-refresh 5 s
        </label>
        <button className="secondary" onClick={load} disabled={!valid}>
          Refresh now
        </button>
      </div>

      <details className="small">
        <summary>Auth experiments (flow 1)</summary>
        <div className="controls">
          <label>
            advertiser id in path{" "}
            <input type="number" min={1} value={pathId} onChange={(e) => setPathId(e.target.value)} style={{ width: 110 }} />
          </label>
          <label>
            token{" "}
            <select value={tokenMode} onChange={(e) => setTokenMode(e.target.value as TokenMode)}>
              <option value="mine">my advertiser token</option>
              <option value="none">no token (→ 401 at the gateway)</option>
              <option value="tampered">bad signature (→ 401 at the gateway)</option>
            </select>
          </label>
        </div>
        <p className="muted">Another advertiser&apos;s id with your token → 403 from the service.</p>
      </details>

      <div className="stats">
        <div className="stat">
          <div className="muted small">{adId ? `ad #${adId}` : "all ads"} · total</div>
          <div className="stat-value">{data ? data.total.toLocaleString() : "—"}</div>
        </div>
        <div className="stat">
          <div className="muted small">data last updated</div>
          <div className="stat-value">{data ? ago(lastUpdated, now) : "—"}</div>
          <div className="muted small">freshness.last_updated_at</div>
        </div>
        <div className="stat">
          <div className="muted small">fetched</div>
          <div className="stat-value">{fetchedAt ? ago(fetchedAt, now) : "—"}</div>
          <div className="small">{res && <ServedBy servedBy={res.servedBy} ms={res.ms} />}</div>
        </div>
      </div>

      <ErrorBox error={error} />
      {error && data ? <p className="muted small">Showing the last successful result (stale).</p> : null}

      {adId && (
        <p>
          Showing ad #{adId}.{" "}
          <button className="secondary" onClick={() => setAdId(null)}>
            ← all ads
          </button>
        </p>
      )}

      {data && (
        <>
          <h2>
            Clicks per {data.granularity} <span className="muted small">(UTC buckets, {bucketLabel(data.from)} → {bucketLabel(data.to)})</span>
          </h2>
          <BarChart points={data.points} label={bucketLabel} />
          <details className="small">
            <summary>Table view ({data.points.length} buckets)</summary>
            <table>
              <thead>
                <tr>
                  <th>bucket start</th>
                  <th>clicks</th>
                </tr>
              </thead>
              <tbody>
                {data.points.map((p) => (
                  <tr key={p.start}>
                    <td>{new Date(p.start).toLocaleString()}</td>
                    <td>{p.clicks}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        </>
      )}

      {byAd && (
        <>
          <h2>Per ad</h2>
          <table>
            <thead>
              <tr>
                <th>ad</th>
                <th>clicks</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {byAd.map((a) => (
                <tr key={a.ad_id}>
                  <td>#{a.ad_id}</td>
                  <td>{a.clicks.toLocaleString()}</td>
                  <td>
                    <button className="secondary" onClick={() => setAdId(a.ad_id)}>
                      series
                    </button>
                  </td>
                </tr>
              ))}
              {byAd.length === 0 && (
                <tr>
                  <td colSpan={3} className="muted">
                    No clicks in range yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}
