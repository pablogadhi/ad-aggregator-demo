// Demo sessions kept in localStorage: a viewer (random user_id) and, optionally, an advertiser.
// Tokens come from the auth service (POST /api/auth/token) and are re-issued shortly before expiry.
import type { components } from "@/api/auth";
import { api } from "@/lib/api";

type TokenRequest = components["schemas"]["TokenRequest"];
type TokenResponse = components["schemas"]["TokenResponse"];

type Stored = { token: string; expiresAt: number };
export type ViewerSession = Stored & { userId: string };
export type AdvertiserSession = Stored & { advertiserId: number };

const VIEWER_KEY = "adagg.viewer";
const USER_KEY = "adagg.userId";
const ADVERTISER_KEY = "adagg.advertiser";
const MARGIN_MS = 60_000;

function read<T>(key: string): T | null {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : null;
  } catch {
    return null;
  }
}

function write(key: string, value: unknown) {
  try {
    if (value === null) localStorage.removeItem(key);
    else localStorage.setItem(key, typeof value === "string" ? value : JSON.stringify(value));
  } catch {
    /* storage unavailable: the session just won't survive a reload */
  }
}

async function issue(req: TokenRequest): Promise<Stored> {
  const { data } = await api<TokenResponse>("auth", "/token", { method: "POST", body: JSON.stringify(req) });
  return { token: data.access_token, expiresAt: Date.now() + data.expires_in * 1000 };
}

const fresh = (s: Stored | null) => !!s && s.expiresAt - Date.now() > MARGIN_MS;

export function getUserId(): string {
  let id: string | null = null;
  try {
    id = localStorage.getItem(USER_KEY);
  } catch {
    /* ignore */
  }
  if (!id) {
    id = `u-${crypto.randomUUID().slice(0, 8)}`;
    write(USER_KEY, id);
  }
  return id;
}

/** Forget the current user id (and its token) so the next click counts as a new user. */
export function newUserId(): string {
  write(USER_KEY, null);
  write(VIEWER_KEY, null);
  return getUserId();
}

export async function viewerSession(force = false): Promise<ViewerSession> {
  const userId = getUserId();
  const cur = read<ViewerSession>(VIEWER_KEY);
  if (!force && cur && cur.userId === userId && fresh(cur)) return cur;
  const s: ViewerSession = { ...(await issue({ role: "viewer", user_id: userId })), userId };
  write(VIEWER_KEY, s);
  return s;
}

export function storedAdvertiser(): AdvertiserSession | null {
  return read<AdvertiserSession>(ADVERTISER_KEY);
}

/** "Log in as" an advertiser id (demo login: the auth service does not check it exists). */
export async function loginAdvertiser(advertiserId: number): Promise<AdvertiserSession> {
  const s: AdvertiserSession = { ...(await issue({ role: "advertiser", advertiser_id: advertiserId })), advertiserId };
  write(ADVERTISER_KEY, s);
  return s;
}

/** The stored advertiser session with a non-expired token (re-issued if needed), or null. */
export async function advertiserSession(): Promise<AdvertiserSession | null> {
  const cur = storedAdvertiser();
  if (!cur) return null;
  return fresh(cur) ? cur : loginAdvertiser(cur.advertiserId);
}

export function logoutAdvertiser() {
  write(ADVERTISER_KEY, null);
}
