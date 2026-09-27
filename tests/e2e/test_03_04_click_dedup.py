"""Flow 3 — click -> redirect, and flow 4 — dedup (spec §5.1, §8.3, §8.4)."""

import uuid

from conftest import Advertiser, click_get, click_post, uid


def _is_uuid(s: str) -> bool:
    try:
        uuid.UUID(s)
        return True
    except (TypeError, ValueError):
        return False


def test_click_redirects_after_accepting(http, advertiser: Advertiser):
    ad = advertiser.create_ad()
    r = click_get(http, ad["id"], uid())
    assert r.status_code == 302, r.text
    assert r.headers["location"] == ad["redirect_url"]
    assert r.headers["x-click-status"] == "accepted"
    assert _is_uuid(r.headers["x-click-id"])
    assert r.headers["x-click-hot"] in ("true", "false")


def test_post_click_returns_json(http, advertiser: Advertiser):
    ad = advertiser.create_ad()
    r = click_post(http, ad["id"], uid())
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "accepted" == r.headers["x-click-status"]
    assert body["redirect_url"] == ad["redirect_url"]
    assert body["ad_id"] == ad["id"]
    assert _is_uuid(body["click_id"]) and body["click_id"] == r.headers["x-click-id"]
    assert body["hot"] is False and r.headers["x-click-hot"] == "false"
    assert body["clicked_at"].endswith("Z") or "+" in body["clicked_at"]


def test_dedup_same_user_same_ad(http, advertiser: Advertiser):
    ad = advertiser.create_ad()
    user = uid()
    first = click_get(http, ad["id"], user)
    assert first.headers["x-click-status"] == "accepted"

    again = click_get(http, ad["id"], user)
    assert again.status_code == 302
    assert again.headers["location"] == ad["redirect_url"]
    assert again.headers["x-click-status"] == "duplicate"
    assert _is_uuid(again.headers["x-click-id"])
    assert again.headers["x-click-id"] != first.headers["x-click-id"]

    via_post = click_post(http, ad["id"], user)  # same code path
    assert via_post.status_code == 200 and via_post.json()["status"] == "duplicate"

    other = click_get(http, ad["id"], uid())
    assert other.status_code == 302 and other.headers["x-click-status"] == "accepted"


def test_same_user_other_ad_is_not_a_duplicate(http, advertiser: Advertiser):
    a, b = advertiser.create_ad(), advertiser.create_ad()
    user = uid()
    assert click_get(http, a["id"], user).headers["x-click-status"] == "accepted"
    assert click_get(http, b["id"], user).headers["x-click-status"] == "accepted"


def test_invalid_click_requests(http):
    assert http.get("/api/click-receiver/click/1", follow_redirects=False).status_code == 422  # no user_id
    assert http.post("/api/click-receiver/clicks", json={"ad_id": 1}).status_code == 422
