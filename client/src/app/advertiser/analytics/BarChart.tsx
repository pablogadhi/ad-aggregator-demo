"use client";
// Minimal single-series bar chart (inline SVG, no deps): thin bars with a 2px gap, rounded tops anchored to
// the baseline, recessive grid, per-bar hover readout. A table view lives next to it on the page.
import { useState } from "react";

export type Bar = { start: string; clicks: number };

const W = 720;
const H = 180;
const PAD = { top: 10, right: 8, bottom: 22, left: 36 };

function niceMax(v: number): number {
  if (v <= 4) return 4;
  const mag = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 2, 2.5, 5, 10]) if (v <= m * mag) return m * mag;
  return 10 * mag;
}

function topRounded(x: number, y: number, w: number, h: number): string {
  const r = Math.min(4, w / 2, h);
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`;
}

export function BarChart({ points, label }: { points: Bar[]; label: (iso: string) => string }) {
  const [hover, setHover] = useState<number | null>(null);
  if (points.length === 0) return <p className="muted">No buckets in range.</p>;

  const max = niceMax(Math.max(...points.map((p) => p.clicks)));
  const iw = W - PAD.left - PAD.right;
  const ih = H - PAD.top - PAD.bottom;
  const band = iw / points.length;
  const bw = Math.max(1, band - 2);
  const y = (v: number) => PAD.top + ih - (v / max) * ih;
  const ticks = [0, max / 2, max];
  const xLabels = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])];
  const h = hover !== null ? points[hover] : null;

  return (
    <figure className="chart">
      <div className="chart-readout small" aria-live="polite">
        {h ? (
          <>
            <strong>{h.clicks}</strong> clicks · bucket {label(h.start)}
          </>
        ) : (
          <span className="muted">Hover a bar for its value</span>
        )}
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Clicks per bucket" onMouseLeave={() => setHover(null)}>
        {ticks.map((t) => (
          <g key={t}>
            <line x1={PAD.left} x2={W - PAD.right} y1={y(t)} y2={y(t)} className={t === 0 ? "axis" : "grid"} />
            <text x={PAD.left - 6} y={y(t)} dy="0.32em" textAnchor="end" className="tick">
              {Number.isInteger(t) ? t : t.toFixed(1)}
            </text>
          </g>
        ))}
        {points.map((p, i) => {
          const x = PAD.left + i * band + (band - bw) / 2;
          const bh = y(0) - y(p.clicks);
          return (
            <g key={p.start} onMouseEnter={() => setHover(i)}>
              {/* hit target: the full column, larger than the mark */}
              <rect x={PAD.left + i * band} y={PAD.top} width={band} height={ih} fill="transparent">
                <title>{`${label(p.start)}: ${p.clicks} clicks`}</title>
              </rect>
              {p.clicks > 0 && (
                <path d={topRounded(x, y(p.clicks), bw, bh)} className={`bar ${hover === i ? "bar-hover" : ""}`} pointerEvents="none" />
              )}
            </g>
          );
        })}
        {xLabels.map((i) => (
          <text key={i} x={PAD.left + i * band + band / 2} y={H - 6} textAnchor={i === 0 ? "start" : i === points.length - 1 ? "end" : "middle"} className="tick">
            {label(points[i].start)}
          </text>
        ))}
      </svg>
    </figure>
  );
}
