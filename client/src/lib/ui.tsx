"use client";
// Small shared bits for the demo pages: served-by badge, error box, polling + clock hooks.
import { useEffect, useRef, useState } from "react";
import { ApiError, describeError } from "@/lib/api";

export function ServedBy({ servedBy, ms }: { servedBy: string | null | undefined; ms?: number }) {
  return (
    <span className="badge" title="X-Served-By: which pod@node answered">
      {servedBy ?? "gateway"}
      {ms !== undefined ? ` · ${ms} ms` : ""}
    </span>
  );
}

export function ErrorBox({ error }: { error: unknown }) {
  if (!error) return null;
  const status = error instanceof ApiError ? error.status : null;
  const who = error instanceof ApiError && error.servedBy ? ` (from ${error.servedBy})` : "";
  return (
    <p className={`alert ${status !== null && status >= 500 ? "alert-5xx" : ""}`} role="alert">
      {describeError(error)}
      {who}
    </p>
  );
}

/** Calls `fn` every `ms` while `enabled` (first call right away if `immediate`); the latest `fn` is always used. */
export function usePolling(fn: () => void | Promise<void>, ms: number, enabled = true, immediate = true) {
  const ref = useRef(fn);
  ref.current = fn;
  useEffect(() => {
    if (!enabled) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    const tick = async () => {
      try {
        await ref.current();
      } finally {
        if (!stopped) timer = setTimeout(tick, ms);
      }
    };
    if (immediate) tick();
    else timer = setTimeout(tick, ms);
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [ms, enabled, immediate]);
}

/** Current time, re-rendering every `ms` (for "N s ago" labels). */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}

export function ago(iso: string | number | null | undefined, now: number): string {
  if (iso === null || iso === undefined) return "never";
  const t = typeof iso === "number" ? iso : Date.parse(iso);
  if (Number.isNaN(t)) return "?";
  const s = Math.max(0, Math.round((now - t) / 1000));
  if (s < 90) return `${s} s ago`;
  if (s < 5400) return `${Math.round(s / 60)} min ago`;
  return `${Math.round(s / 3600)} h ago`;
}

export function fmtTime(iso: string | null | undefined): string {
  return iso ? new Date(iso).toLocaleTimeString() : "—";
}
