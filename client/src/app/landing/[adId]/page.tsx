"use client";
// Default landing page the demo ads redirect to (end of flow §8.3: click → 302 → here).
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import type { components } from "@/api/ad-placement";
import { api } from "@/lib/api";
import { viewerSession } from "@/lib/session";
import { ErrorBox } from "@/lib/ui";

type Ad = components["schemas"]["Ad"];

export default function Landing() {
  const { adId } = useParams<{ adId: string }>();
  const id = Number(adId);
  const [ad, setAd] = useState<Ad | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [arrived] = useState(() => new Date());

  useEffect(() => {
    if (!(id > 0)) return;
    viewerSession()
      .then((s) => api<Ad>("ad-placement", `/ads/${id}`, { token: s.token }))
      .then((r) => setAd(r.data))
      .catch(setError);
  }, [id]);

  return (
    <div className="landing">
      <h1>You came from ad #{Number.isFinite(id) ? id : adId}</h1>
      <p className="muted">
        The click receiver recorded the click (Kafka ack) and answered 302 to this page at{" "}
        {arrived.toLocaleTimeString()}. It shows up in the advertiser&apos;s analytics within ~60 s.
      </p>
      {ad && (
        <div className="card">
          <strong>{ad.content}</strong>
          <p className="muted small">advertiser {ad.advertiser_id}</p>
        </div>
      )}
      <ErrorBox error={error} />
      <p className="row">
        <a href="/">Back to the feed</a>
        <a href="/advertiser/analytics">Advertiser analytics</a>
      </p>
    </div>
  );
}
