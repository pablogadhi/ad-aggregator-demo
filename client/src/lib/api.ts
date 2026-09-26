// Tiny typed fetch helper over the generated contract types (src/api/*.ts, from `pnpm gen:api`).
// Every call goes to a relative /api/<service>/... URL:
//   - in the cluster the gateway routes it to the service
//   - with `pnpm dev` next.config.ts forwards it to the gateway on localhost:8080

export class ApiError extends Error {
  constructor(
    public status: number,
    public body: unknown,
    public servedBy: string | null = null,
  ) {
    super(`HTTP ${status}`);
  }

  /** `detail` from the contract's Error schema (or FastAPI's 422 list), else a generic message. */
  get detail(): string {
    const b = this.body as { detail?: unknown } | null;
    if (b && typeof b.detail === "string") return b.detail;
    if (b && b.detail !== undefined) return JSON.stringify(b.detail);
    if (this.status === 0) return "network error (gateway unreachable?)";
    return this.message;
  }
}

export type Served<T> = {
  data: T;
  /** X-Served-By (pod@node) — which replica answered; null when the gateway itself answered. */
  servedBy: string | null;
  headers: Headers;
  /** Round-trip time in ms as seen by the browser. */
  ms: number;
};

export type ApiInit = RequestInit & {
  /** JWT sent as `Authorization: Bearer <token>` (ad-placement, analytics). */
  token?: string | null;
};

export async function api<T>(service: string, path: string, init?: ApiInit): Promise<Served<T>> {
  const { token, headers, ...rest } = init ?? {};
  const started = performance.now();
  let res: Response;
  try {
    res = await fetch(`/api/${service}${path}`, {
      ...rest,
      headers: {
        "content-type": "application/json",
        ...(token ? { authorization: `Bearer ${token}` } : {}),
        ...headers,
      },
      cache: "no-store",
    });
  } catch (e) {
    throw new ApiError(0, { detail: String(e) });
  }
  const body = res.status === 204 ? null : await res.json().catch(() => null);
  const servedBy = res.headers.get("x-served-by");
  if (!res.ok) throw new ApiError(res.status, body, servedBy);
  // X-Served-By shows which pod@node answered — useful for watching load balancing and failover
  return { data: body as T, servedBy, headers: res.headers, ms: Math.round(performance.now() - started) };
}

/** Human description of a failed call, with a hint for the status classes the demo cares about. */
export function describeError(e: unknown): string {
  if (!(e instanceof ApiError)) return String(e);
  const hint =
    e.status === 0
      ? ""
      : e.status === 401
        ? " — no valid token (rejected at the gateway or by the service)"
        : e.status === 403
          ? " — authenticated, but not the owner of this resource"
          : e.status === 404
            ? " — not found"
            : e.status === 503
              ? " — dependency unavailable (e.g. Kafka ack failed); safe to retry"
              : e.status >= 500
                ? " — server/dependency error"
                : "";
  return `${e.status === 0 ? "network" : `HTTP ${e.status}`}: ${e.detail}${hint}`;
}
