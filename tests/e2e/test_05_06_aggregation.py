"""Flow 5 — aggregation within 60 s, and flow 6 — rollups (spec §6.1, §8.5, §8.6)."""

import time
from collections import Counter
from datetime import timedelta

from conftest import Advertiser, click_post, eventually, minute_of, now, parse_ts, uid, window


def _click_n(http, ad_id: int, n: int, users: list | None = None) -> list:
    """n accepted clicks by distinct users; returns their minute buckets (from clicked_at)."""
    minutes = []
    for _ in range(n):
        user = uid()
        r = click_post(http, ad_id, user)
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "accepted"
        minutes.append(minute_of(parse_ts(r.json()["clicked_at"])))
        if users is not None:
            users.append(user)
    return minutes


def test_clicks_are_aggregated_within_60s(http, advertiser: Advertiser):
    start = now()
    a, b = advertiser.create_ad("A"), advertiser.create_ad("B")
    users_a: list = []
    minutes_a = _click_n(http, a["id"], 7, users_a)
    minutes_b = _click_n(http, b["id"], 3)
    # 2 duplicates: users who already clicked A click it again (redirected, not counted)
    for user in users_a[:2]:
        r = click_post(http, a["id"], user)
        assert r.status_code == 200 and r.json()["status"] == "duplicate", r.text
    last_click = time.monotonic()
    expected_a, expected_b = len(minutes_a), len(minutes_b)  # 7 and 3
    frm, to = window(start)

    def per_ad():
        r = advertiser.ad_clicks(a["id"], frm, to)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["total"] == expected_a, f"ad A total {body['total']} != {expected_a}"
        return body

    body = eventually(per_ad, timeout=90, interval=0.5)
    lag = time.monotonic() - last_click
    print(f"\n[flow 5] last click -> visible in analytics: {lag:.1f}s")
    assert lag < 60, f"eventual consistency budget exceeded: {lag:.1f}s"

    # minute buckets match the click times
    got = {parse_ts(p["start"]): p["clicks"] for p in body["points"] if p["clicks"]}
    assert got == dict(Counter(minutes_a)), (got, Counter(minutes_a))
    assert body["freshness"]["last_updated_at"] is not None

    def per_advertiser():
        r = advertiser.clicks(frm, to)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["total"] == expected_a + expected_b
        assert {t["ad_id"]: t["clicks"] for t in body["by_ad"]} == {a["id"]: expected_a, b["id"]: expected_b}
        assert body["by_ad"][0]["ad_id"] == a["id"]  # clicks descending
        return body

    body = eventually(per_advertiser, timeout=30)
    assert sum(p["clicks"] for p in body["points"]) == expected_a + expected_b


def test_rollups_match_minutes(http, advertiser: Advertiser):
    ad = advertiser.create_ad()
    minutes = _click_n(http, ad["id"], 4)
    # a range on hour boundaries, so minute and hour granularity cover exactly the same span
    frm = min(minutes).replace(minute=0) - timedelta(hours=1)
    to = max(minutes).replace(minute=0) + timedelta(hours=1)

    def minute_total():
        r = advertiser.ad_clicks(ad["id"], frm, to, "minute")
        assert r.status_code == 200, r.text
        assert r.json()["total"] == 4
        return r.json()

    by_minute = eventually(minute_total, timeout=90)
    by_hour = advertiser.ad_clicks(ad["id"], frm, to, "hour").json()
    by_day = advertiser.ad_clicks(ad["id"], frm, to, "day").json()
    assert by_hour["total"] == by_minute["total"] == by_day["total"] == 4
    assert len(by_minute["points"]) == (to - frm) // timedelta(minutes=1)
    assert len(by_hour["points"]) == (to - frm) // timedelta(hours=1)
    assert sum(p["clicks"] for p in by_hour["points"]) == 4


def test_too_many_buckets_is_400(http, advertiser: Advertiser):
    to = now()
    r = advertiser.clicks(to - timedelta(minutes=1441), to, "minute")
    assert r.status_code == 400, r.text
    r = advertiser.clicks(to - timedelta(hours=25), to, "hour")  # 26 buckets: fine
    assert r.status_code == 200, r.text


def test_from_after_to_is_400_and_naive_time_is_422(http, advertiser: Advertiser):
    t = now()
    assert advertiser.clicks(t, t - timedelta(minutes=5)).status_code == 400
    r = http.get(
        f"/api/analytics/advertisers/{advertiser.id}/clicks",
        params={"from": "2026-01-01T00:00:00"},
        headers=advertiser.headers,
    )
    assert r.status_code == 422, r.text
