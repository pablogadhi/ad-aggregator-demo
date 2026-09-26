"use client";
// Ad feed (viewer). Flows: §8.2 (GET /ads with click_url), §8.3 click → redirect, §8.4 dedup, §8.7 unknown/deleted ad.
import { useCallback, useEffect, useState } from "react";
import type { components as Placement } from "@/api/ad-placement";
import type { components as Receiver } from "@/api/click-receiver";
import { api, ApiError, type Served } from "@/lib/api";
import { getUserId, newUserId, viewerSession } from "@/lib/session";
import { ErrorBox, ServedBy, fmtTime } from "@/lib/ui";

type Ad = Placement["schemas"]["Ad"];
type AdPage = Placement["schemas"]["AdPage"];
type ClickResult = Receiver["schemas"]["ClickResult"];
type ClickOutcome = { at: number; result?: Served<ClickResult>; error?: unknown };

const PAGE = 50;

export default function Feed() {
  const [userId, setUserId] = useState<string | null>(null);
  const [ads, setAds] = useState<Ad[]>([]);
  const [next, setNext] = useState<number | null>(null);
  const [meta, setMeta] = useState<{ servedBy: string | null; ms: number } | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [clicks, setClicks] = useState<Record<number, ClickOutcome>>({});
  const [manualId, setManualId] = useState("");

  const load = useCallback(async (after: number | null) => {
    setLoading(true);
    setError(null);
    try {
      const q = `?limit=${PAGE}${after ? `&after_id=${after}` : ""}`;
      let s = await viewerSession();
      let res: Served<AdPage>;
      try {
        res = await api<AdPage>("ad-placement", `/ads${q}`, { token: s.token });
      } catch (e) {
        if (!(e instanceof ApiError && e.status === 401)) throw e;
        s = await viewerSession(true); // token rejected (e.g. key rotated): get a new one once
        res = await api<AdPage>("ad-placement", `/ads${q}`, { token: s.token });
      }
      setAds((prev) => (after ? [...prev, ...res.data.items] : res.data.items));
      setNext(res.data.next_after_id);
      setMeta({ servedBy: res.servedBy, ms: res.ms });
    } catch (e) {
      setError(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    setUserId(getUserId());
    load(null);
  }, [load]);

  async function clickViaApi(adId: number) {
    if (!userId) return;
    try {
      const result = await api<ClickResult>("click-receiver", "/clicks", {
        method: "POST",
        body: JSON.stringify({ ad_id: adId, user_id: userId }),
      });
      setClicks((c) => ({ ...c, [adId]: { at: Date.now(), result } }));
    } catch (error) {
      setClicks((c) => ({ ...c, [adId]: { at: Date.now(), error } }));
    }
  }

  function switchUser() {
    setUserId(newUserId());
    setClicks({});
  }

  const manual = Number(manualId);

  return (
    <>
      <h1>Ad feed</h1>
      <p className="muted">
        Viewing as <code>{userId ?? "…"}</code>{" "}
        <button className="secondary" onClick={switchUser} title="New random user_id: the next click counts again">
          new user
        </button>{" "}
        A user&apos;s repeated click on the same ad within 10 min is a <em>duplicate</em>: it still redirects but
        isn&apos;t counted.
      </p>

      <div className="row">
        <button onClick={() => load(null)} disabled={loading}>
          {loading ? "Loading…" : "Refresh"}
        </button>
        {meta && (
          <span className="muted">
            ad-placement <ServedBy servedBy={meta.servedBy} ms={meta.ms} />
          </span>
        )}
      </div>
      <ErrorBox error={error} />

      {ads.length === 0 && !loading && !error && (
        <p className="muted">
          No active ads yet. Create some on the <a href="/advertiser">advertiser</a> page.
        </p>
      )}

      <ul className="cards">
        {ads.map((ad) => (
          <li key={ad.id} className="card">
            <div className="row spread">
              <strong>
                #{ad.id} · {ad.content}
              </strong>
              <span className="muted small">advertiser {ad.advertiser_id}</span>
            </div>
            {ad.img_url && (
              <img src={ad.img_url} alt="" className="ad-img" />
            )}
            <div className="row">
              <a href={userId ? `${ad.click_url}?user_id=${encodeURIComponent(userId)}` : undefined}>
                Open ad (302 redirect)
              </a>
              <button className="secondary" onClick={() => clickViaApi(ad.id)} disabled={!userId}>
                Click via API
              </button>
              <span className="muted small">→ {ad.redirect_url}</span>
            </div>
            <ClickLine outcome={clicks[ad.id]} />
          </li>
        ))}
      </ul>
      {next !== null && (
        <button className="secondary" onClick={() => load(next)} disabled={loading}>
          Load more
        </button>
      )}

      <h2>Click any ad id</h2>
      <p className="muted">Try an id that doesn&apos;t exist or was deactivated: 404, nothing is recorded.</p>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (manual > 0) clickViaApi(manual);
        }}
      >
        <input type="number" min={1} placeholder="ad id" value={manualId} onChange={(e) => setManualId(e.target.value)} />
        <button type="submit" disabled={!(manual > 0)}>
          Click via API
        </button>
      </form>
      {manual > 0 && !ads.some((a) => a.id === manual) && <ClickLine outcome={clicks[manual]} />}
    </>
  );
}

function ClickLine({ outcome }: { outcome?: ClickOutcome }) {
  if (!outcome) return null;
  if (outcome.error) return <ErrorBox error={outcome.error} />;
  const r = outcome.result!;
  return (
    <p className="click-result small">
      <span className={`pill ${r.data.status === "accepted" ? "pill-ok" : "pill-warn"}`}>{r.data.status}</span>{" "}
      {r.data.hot && <span className="pill pill-hot" title="X-Click-Hot: the receiver treated this ad as hot">hot</span>} click_id <code>{r.data.click_id}</code> at{" "}
      {fmtTime(r.data.clicked_at)} · receiver <ServedBy servedBy={r.servedBy} ms={r.ms} />
    </p>
  );
}
