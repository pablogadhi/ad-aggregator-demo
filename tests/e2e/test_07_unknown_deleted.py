"""Flow 7 — unknown / deleted ad (spec §8.7)."""

from conftest import Advertiser, bearer, click_get, click_post, eventually, now, token, uid, window

UNKNOWN_AD = 2_000_000_000  # int32-safe, never created by the lab


def test_unknown_ad_is_404_and_not_counted(http, advertiser: Advertiser):
    start = now()
    r = click_get(http, UNKNOWN_AD, uid())
    assert r.status_code == 404, r.text
    assert "location" not in r.headers
    assert "x-click-id" in r.headers
    assert click_post(http, UNKNOWN_AD, uid()).status_code == 404
    frm, to = window(start)
    body = advertiser.ad_clicks(UNKNOWN_AD, frm, to).json()
    assert body["total"] == 0  # zero-filled series, not 404
    assert advertiser.clicks(frm, to).json()["total"] == 0


def test_soft_deleted_ad_disappears_from_feed(http, advertiser: Advertiser):
    ad = advertiser.create_ad()
    viewer = bearer(token(http, role="viewer", user_id=uid()))

    def first_after(ad_id):
        r = http.get("/api/ad-placement/ads", params={"after_id": ad_id - 1, "limit": 1}, headers=viewer)
        assert r.status_code == 200, r.text
        items = r.json()["items"]
        return items[0]["id"] if items else None

    eventually(lambda: _assert_eq(first_after(ad["id"]), ad["id"]), timeout=15)

    r = http.delete(f"/api/ad-placement/advertisers/{advertiser.id}/ads/{ad['id']}", headers=advertiser.headers)
    assert r.status_code == 204, r.text
    # idempotent
    r = http.delete(f"/api/ad-placement/advertisers/{advertiser.id}/ads/{ad['id']}", headers=advertiser.headers)
    assert r.status_code == 204, r.text

    eventually(lambda: _assert_ne(first_after(ad["id"]), ad["id"]), timeout=15)
    eventually(lambda: _assert_eq(http.get(f"/api/ad-placement/ads/{ad['id']}", headers=viewer).status_code, 404), timeout=15)

    # the owner still sees it, inactive
    items = http.get(f"/api/ad-placement/advertisers/{advertiser.id}/ads", headers=advertiser.headers).json()["items"]
    assert [a["active"] for a in items if a["id"] == ad["id"]] == [False]


def _assert_eq(a, b):
    assert a == b, (a, b)


def _assert_ne(a, b):
    assert a != b, (a, b)
