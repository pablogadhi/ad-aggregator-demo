"""Flow 2 — advertiser onboarding (spec §8.2)."""

from conftest import Advertiser, bearer, eventually, token, uid


def test_onboarding_and_listing(http):
    adv = Advertiser(http)  # POST /advertisers with a viewer token -> 201 (asserted inside)
    ads = [adv.create_ad("first"), adv.create_ad("second")]
    ids = sorted(a["id"] for a in ads)
    for ad in ads:
        assert ad["advertiser_id"] == adv.id
        assert ad["active"] is True
        assert ad["click_url"] == f"/api/click-receiver/click/{ad['id']}"

    r = http.get(f"/api/ad-placement/advertisers/{adv.id}/ads", headers=adv.headers)
    assert r.status_code == 200, r.text
    assert sorted(a["id"] for a in r.json()["items"]) == ids

    r = http.get(f"/api/ad-placement/advertisers/{adv.id}", headers=adv.headers)
    assert r.status_code == 200 and r.json()["id"] == adv.id

    viewer = bearer(token(http, role="viewer", user_id=uid()))

    def listed():
        # keyset pagination: start right before our first ad
        r = http.get("/api/ad-placement/ads", params={"after_id": ids[0] - 1, "limit": 2}, headers=viewer)
        assert r.status_code == 200, r.text
        page = {a["id"]: a for a in r.json()["items"]}
        for ad_id in ids:
            assert ad_id in page, f"ad {ad_id} not (yet) in GET /ads"
            assert page[ad_id]["click_url"] == f"/api/click-receiver/click/{ad_id}"
        return page

    eventually(listed, timeout=15)  # GET /ads reads the replica

    r = http.get(f"/api/ad-placement/ads/{ids[0]}", headers=viewer)
    assert r.status_code == 200 and r.json()["id"] == ids[0]


def test_advertiser_cannot_create_ads_for_others(http):
    a, b = Advertiser(http), Advertiser(http)
    r = http.post(
        f"/api/ad-placement/advertisers/{b.id}/ads",
        json={"content": "x", "redirect_url": "http://localhost:8080/landing/1"},
        headers=a.headers,
    )
    assert r.status_code == 403, r.text
