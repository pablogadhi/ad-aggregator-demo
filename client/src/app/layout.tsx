import type { Metadata } from "next";
import type { ReactNode } from "react";
import "./globals.css";

export const metadata: Metadata = {
  title: "Ad Click Aggregator",
  description: "Demo client for the ad-aggregator design running in the local cluster",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header>
          <strong>
            <a href="/" className="brand">
              ad-aggregator
            </a>
          </strong>
          <nav>
            <a href="/">Feed</a>
            <a href="/advertiser">Advertiser</a>
            <a href="/advertiser/analytics">Analytics</a>
            <a href="/hot-ads">Hot ads</a>
            <a href="http://grafana.localhost:8080" target="_blank" rel="noreferrer">
              Grafana
            </a>
            <a href="http://chaos.localhost:8080" target="_blank" rel="noreferrer">
              Chaos Mesh
            </a>
          </nav>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
