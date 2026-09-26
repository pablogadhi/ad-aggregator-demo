"use client";
// Advertiser console. Flows: §8.2 onboarding (sign up → advertiser token → create ads → list), §8.7 soft delete,
// §8.1 owner-only access (403 when the token's advertiser_id doesn't match the path).
import { useCallback, useEffect, useState } from "react";
import type { components } from "@/api/ad-placement";
import { api, type Served } from "@/lib/api";
import {
  advertiserSession,
  loginAdvertiser,
  logoutAdvertiser,
  storedAdvertiser,
  viewerSession,
  type AdvertiserSession,
} from "@/lib/session";
import { ErrorBox, ServedBy, fmtTime } from "@/lib/ui";

type S = components["schemas"];
type Ad = S["Ad"];
type Advertiser = S["Advertiser"];
type AdCreate = S["AdCreate"];
type AdUpdate = S["AdUpdate"];

export default function AdvertiserPage() {
  const [session, setSession] = useState<AdvertiserSession | null>(null);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [name, setName] = useState("");
  const [loginId, setLoginId] = useState("");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    setSession(storedAdvertiser());
    setReady(true);
  }, []);

  async function run(fn: () => Promise<void>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  }

  const signUp = () =>
    run(async () => {
      // Sign-up needs any valid token: use the viewer's.
      const viewer = await viewerSession();
      const { data } = await api<Advertiser>("ad-placement", "/advertisers", {
        method: "POST",
        token: viewer.token,
        body: JSON.stringify({ name } satisfies S["AdvertiserCreate"]),
      });
      setSession(await loginAdvertiser(data.id));
      setName("");
    });

  const logIn = () =>
    run(async () => {
      setSession(await loginAdvertiser(Number(loginId)));
      setLoginId("");
    });

  if (!ready) return null;

  return (
    <>
      <h1>Advertiser</h1>
      {session ? (
        <div className="row">
          <span>
            Logged in as advertiser <strong>#{session.advertiserId}</strong>
          </span>
          <a href="/advertiser/analytics">Analytics →</a>
          <button
            className="secondary"
            onClick={() => {
              logoutAdvertiser();
              setSession(null);
            }}
          >
            Log out
          </button>
        </div>
      ) : (
        <p className="muted">Demo login: no passwords. Sign up, or log in as any advertiser id.</p>
      )}

      <div className="grid2">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            signUp();
          }}
        >
          <input placeholder="New advertiser name" value={name} onChange={(e) => setName(e.target.value)} required maxLength={200} />
          <button type="submit" disabled={busy || !name.trim()}>
            Sign up
          </button>
        </form>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            logIn();
          }}
        >
          <input type="number" min={1} placeholder="Advertiser id" value={loginId} onChange={(e) => setLoginId(e.target.value)} required />
          <button type="submit" disabled={busy || !(Number(loginId) > 0)}>
            Log in as
          </button>
        </form>
      </div>
      <ErrorBox error={error} />

      {session && <ManageAds key={session.advertiserId} advertiserId={session.advertiserId} />}
    </>
  );
}

function ManageAds({ advertiserId }: { advertiserId: number }) {
  const base = `/advertisers/${advertiserId}`;
  const [advertiser, setAdvertiser] = useState<Advertiser | null>(null);
  const [ads, setAds] = useState<Ad[]>([]);
  const [meta, setMeta] = useState<Pick<Served<unknown>, "servedBy" | "ms"> | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [form, setForm] = useState({ content: "", img_url: "", redirect_url: "" });
  const [busy, setBusy] = useState(false);

  const call = useCallback(async <T,>(path: string, init?: RequestInit) => {
    const s = await advertiserSession();
    if (!s) throw new Error("not logged in");
    const r = await api<T>("ad-placement", path, { ...init, token: s.token });
    setMeta({ servedBy: r.servedBy, ms: r.ms });
    return r.data;
  }, []);

  const refresh = useCallback(async () => {
    setError(null);
    try {
      const [adv, list] = await Promise.all([
        call<Advertiser>(base),
        call<{ items: Ad[] }>(`${base}/ads?include_inactive=true`),
      ]);
      setAdvertiser(adv);
      setAds(list.items);
    } catch (e) {
      setError(e);
    }
  }, [base, call]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  async function act(fn: () => Promise<unknown>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
      await refresh();
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  }

  const create = () =>
    act(async () => {
      const landing = (id: number | string) => `${window.location.origin}/landing/${id}`;
      const custom = form.redirect_url.trim();
      const body: AdCreate = {
        content: form.content,
        img_url: form.img_url.trim() || null,
        redirect_url: custom || landing("new"),
      };
      const ad = await call<Ad>(`${base}/ads`, { method: "POST", body: JSON.stringify(body) });
      if (!custom) {
        // The id is only known after creation: point the default landing page at it.
        const update: AdUpdate = { content: ad.content, img_url: ad.img_url, redirect_url: landing(ad.id), active: true };
        await call<Ad>(`${base}/ads/${ad.id}`, { method: "PUT", body: JSON.stringify(update) });
      }
      setForm({ content: "", img_url: "", redirect_url: "" });
    });

  const setActive = (ad: Ad, active: boolean) =>
    act(() =>
      active
        ? call<Ad>(`${base}/ads/${ad.id}`, {
            method: "PUT",
            body: JSON.stringify({ content: ad.content, img_url: ad.img_url, redirect_url: ad.redirect_url, active } satisfies AdUpdate),
          })
        : call<null>(`${base}/ads/${ad.id}`, { method: "DELETE" }),
    );

  return (
    <>
      <h2>
        {advertiser ? advertiser.name : `Advertiser #${advertiserId}`}
        {advertiser && <span className="muted small"> · since {new Date(advertiser.created_at).toLocaleDateString()}</span>}
      </h2>

      <form
        className="stack"
        onSubmit={(e) => {
          e.preventDefault();
          create();
        }}
      >
        <input
          placeholder="Ad text"
          value={form.content}
          onChange={(e) => setForm({ ...form, content: e.target.value })}
          required
          maxLength={2000}
        />
        <input
          type="url"
          placeholder="Image URL (optional)"
          value={form.img_url}
          onChange={(e) => setForm({ ...form, img_url: e.target.value })}
        />
        <input
          type="url"
          placeholder="Redirect URL (default: this site's /landing/<ad id>)"
          value={form.redirect_url}
          onChange={(e) => setForm({ ...form, redirect_url: e.target.value })}
        />
        <button type="submit" disabled={busy || !form.content.trim()}>
          Create ad
        </button>
      </form>

      <div className="row">
        <button className="secondary" onClick={() => act(async () => {})} disabled={busy}>
          Refresh
        </button>
        {meta && (
          <span className="muted">
            ad-placement <ServedBy servedBy={meta.servedBy} ms={meta.ms} />
          </span>
        )}
      </div>
      <ErrorBox error={error} />

      <table>
        <thead>
          <tr>
            <th>id</th>
            <th>ad</th>
            <th>redirect</th>
            <th>updated</th>
            <th>status</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {ads.map((ad) => (
            <tr key={ad.id} className={ad.active ? "" : "inactive"}>
              <td>{ad.id}</td>
              <td>{ad.content}</td>
              <td className="small wrap">{ad.redirect_url}</td>
              <td className="small">{fmtTime(ad.updated_at)}</td>
              <td>
                <span className={`pill ${ad.active ? "pill-ok" : "pill-off"}`}>{ad.active ? "active" : "inactive"}</span>
              </td>
              <td>
                {ad.active ? (
                  <button className="secondary" onClick={() => setActive(ad, false)} disabled={busy}>
                    Deactivate
                  </button>
                ) : (
                  <button className="secondary" onClick={() => setActive(ad, true)} disabled={busy}>
                    Reactivate
                  </button>
                )}
              </td>
            </tr>
          ))}
          {ads.length === 0 && (
            <tr>
              <td colSpan={6} className="muted">
                No ads yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <p className="muted small">
        Deactivating is a soft delete: the ad leaves the feed at once, but receivers cache ads for up to 60 s, so a
        click may still be accepted briefly.
      </p>
    </>
  );
}
