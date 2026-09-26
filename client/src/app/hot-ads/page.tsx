"use client";
// Hot-ad detection (flow §8.8): each receiver's in-process view of hot ads, refreshed from Redis every ~2 s.
// Polling every 2 s through the gateway lands on different receiver pods — their views may differ briefly.
import { useState } from "react";
import type { components } from "@/api/click-receiver";
import { api, type Served } from "@/lib/api";
import { ErrorBox, ServedBy, ago, usePolling, useNow } from "@/lib/ui";

type HotAds = components["schemas"]["HotAds"];
type Seen = { servedBy: string; at: number; count: number };

const POLL_MS = 2000;

export default function HotAdsPage() {
  const [res, setRes] = useState<Served<HotAds> | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [pods, setPods] = useState<Record<string, Seen>>({});
  const [paused, setPaused] = useState(false);
  const now = useNow();

  usePolling(
    async () => {
      try {
        const r = await api<HotAds>("click-receiver", "/hot-ads");
        setRes(r);
        setError(null);
        const pod = r.servedBy ?? "unknown";
        setPods((p) => ({ ...p, [pod]: { servedBy: pod, at: Date.now(), count: r.data.items.length } }));
      } catch (e) {
        setError(e);
      }
    },
    POLL_MS,
    !paused,
  );

  const d = res?.data;
  const sorted = d ? [...d.items].sort((a, b) => Number(b.permanent) - Number(a.permanent) || b.marks - a.marks) : [];

  return (
    <>
      <h1>Hot ads</h1>
      <p className="muted">
        An ad with ≥ threshold accepted clicks in the last 10 min is marked hot for 10 min and its Kafka key is salted
        over several partitions. After 10 markings it stays hot permanently.
      </p>

      <div className="row">
        <button className="secondary" onClick={() => setPaused(!paused)}>
          {paused ? "Resume polling" : "Pause polling"}
        </button>
        {res && (
          <span className="muted">
            answered by <ServedBy servedBy={res.servedBy} ms={res.ms} />
          </span>
        )}
      </div>
      <ErrorBox error={error} />

      {d && (
        <div className="stats">
          <div className="stat">
            <div className="muted small">threshold</div>
            <div className="stat-value">{d.threshold_clicks_10m.toLocaleString()}</div>
            <div className="muted small">clicks / 10 min</div>
          </div>
          <div className="stat">
            <div className="muted small">salting</div>
            <div className="stat-value">{d.salting_enabled ? "on" : "off"}</div>
            <div className="muted small">{d.salt_buckets} salt buckets</div>
          </div>
          <div className="stat">
            <div className="muted small">hot / permanent</div>
            <div className="stat-value">
              {d.items.length} / {d.items.filter((i) => i.permanent).length}
            </div>
          </div>
          <div className="stat">
            <div className="muted small">receiver refreshed from Redis</div>
            <div className="stat-value">{ago(d.refreshed_at, now)}</div>
            <div className="muted small">stale = Redis unreachable, last known set kept</div>
          </div>
        </div>
      )}

      <table>
        <thead>
          <tr>
            <th>ad</th>
            <th>state</th>
            <th>marks</th>
            <th>hot until</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((a) => {
            const left = a.hot_until ? Math.round((Date.parse(a.hot_until) - now) / 1000) : null;
            return (
              <tr key={a.ad_id}>
                <td>#{a.ad_id}</td>
                <td>
                  <span className={`pill ${a.permanent ? "pill-perm" : "pill-hot"}`}>{a.permanent ? "permanent" : "hot"}</span>
                </td>
                <td>{a.marks}</td>
                <td className="small">
                  {a.hot_until ? `${new Date(a.hot_until).toLocaleTimeString()} (${left !== null && left > 0 ? `in ${left} s` : "expired"})` : "—"}
                </td>
              </tr>
            );
          })}
          {d && sorted.length === 0 && (
            <tr>
              <td colSpan={4} className="muted">
                No hot ads right now.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {Object.keys(pods).length > 0 && (
        <>
          <h2>Receivers seen</h2>
          <p className="muted small">Each receiver keeps its own cached view; a new hot ad reaches all of them within ~3 s.</p>
          <table>
            <thead>
              <tr>
                <th>pod@node</th>
                <th>hot ads in its view</th>
                <th>last answered</th>
              </tr>
            </thead>
            <tbody>
              {Object.values(pods)
                .sort((a, b) => a.servedBy.localeCompare(b.servedBy))
                .map((p) => (
                  <tr key={p.servedBy}>
                    <td>
                      <code>{p.servedBy}</code>
                    </td>
                    <td>{p.count}</td>
                    <td className="small">{ago(p.at, now)}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}
