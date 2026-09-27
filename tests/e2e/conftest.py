import os
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

BASE_URL = os.environ.get("SDL_GATEWAY_URL", "http://localhost:8080")


@pytest.fixture(scope="session")
def http():
    with httpx.Client(base_url=BASE_URL, timeout=10) as client:
        yield client


def eventually(fn, timeout: float = 30, interval: float = 0.5):
    """Retry an assertion until it passes — for eventually-consistent flows (replicas, streams, caches)."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return fn()
        except AssertionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(interval)


# --- helpers shared by the acceptance flows (design/spec.md §8) -------------------------------
def uid(prefix: str = "u") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def minute_of(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(second=0, microsecond=0)


def token(http: httpx.Client, **body) -> str:
    r = http.post("/api/auth/token", json=body)
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


def bearer(tok: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {tok}"}


class Advertiser:
    """A fresh advertiser (sign-up with a viewer token) plus its own advertiser token."""

    def __init__(self, http: httpx.Client):
        self.http = http
        viewer = token(http, role="viewer", user_id=uid("signup"))
        r = http.post("/api/ad-placement/advertisers", json={"name": uid("adv")}, headers=bearer(viewer))
        assert r.status_code == 201, r.text
        self.id: int = r.json()["id"]
        self.token = token(http, role="advertiser", advertiser_id=self.id)
        self.headers = bearer(self.token)

    def create_ad(self, content: str = "e2e ad") -> dict:
        body = {"content": content, "redirect_url": f"http://localhost:8080/landing/{uid('x')}"}
        r = self.http.post(f"/api/ad-placement/advertisers/{self.id}/ads", json=body, headers=self.headers)
        assert r.status_code == 201, r.text
        ad = r.json()
        assert ad["redirect_url"] == body["redirect_url"]
        return ad

    def ad_clicks(self, ad_id: int, frm: datetime, to: datetime, granularity: str = "minute") -> httpx.Response:
        return self.http.get(
            f"/api/analytics/advertisers/{self.id}/ads/{ad_id}/clicks",
            params={"from": iso(frm), "to": iso(to), "granularity": granularity},
            headers=self.headers,
        )

    def clicks(self, frm: datetime, to: datetime, granularity: str = "minute") -> httpx.Response:
        return self.http.get(
            f"/api/analytics/advertisers/{self.id}/clicks",
            params={"from": iso(frm), "to": iso(to), "granularity": granularity},
            headers=self.headers,
        )


@pytest.fixture
def advertiser(http) -> Advertiser:
    return Advertiser(http)


def click_get(http: httpx.Client, ad_id: int, user_id: str) -> httpx.Response:
    return http.get(f"/api/click-receiver/click/{ad_id}", params={"user_id": user_id}, follow_redirects=False)


def click_post(http: httpx.Client, ad_id: int, user_id: str) -> httpx.Response:
    return http.post("/api/click-receiver/clicks", json={"ad_id": ad_id, "user_id": user_id})


def now() -> datetime:
    return datetime.now(UTC)


def window(start: datetime, minutes_after: int = 2) -> tuple[datetime, datetime]:
    """An analytics range that surely contains clicks made since `start`."""
    return start - timedelta(minutes=1), now() + timedelta(minutes=minutes_after)
