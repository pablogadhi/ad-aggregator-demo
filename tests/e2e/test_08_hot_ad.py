"""Flow 8 — hot ad (spec §5.1.1, §8.8): 1,300 accepted clicks mark a fresh ad hot, its clicks get
salted, and the two-stage aggregation still counts them exactly."""

import time
from concurrent.futures import ThreadPoolExecutor

import httpx
from conftest import BASE_URL, Advertiser, click_get, eventually, now, uid, window

N_CLICKS = 1300
CONCURRENCY = 50


def test_hot_ad_is_marked_salted_and_counted_exactly(advertiser: Advertiser):
    start = now()
    ad = advertiser.create_ad("viral")
    statuses: dict[str, int] = {}

    with httpx.Client(
        base_url=BASE_URL, timeout=10, limits=httpx.Limits(max_connections=CONCURRENCY + 10)
    ) as http:

        def one(_):
            # a 503 means "not recorded, retry" (spec §5.1 step 4): retry with the same user
            user = uid()
            for _attempt in range(5):
                r = click_get(http, ad["id"], user)
                if r.status_code == 302:
                    return r.headers["x-click-status"]
                time.sleep(0.2)
            return f"http-{r.status_code}"

        t0 = time.monotonic()
        with ThreadPoolExecutor(CONCURRENCY) as pool:
            for s in pool.map(one, range(N_CLICKS)):
                statuses[s] = statuses.get(s, 0) + 1
        elapsed = time.monotonic() - t0
        print(f"\n[flow 8] {N_CLICKS} clicks in {elapsed:.1f}s: {statuses}")
        accepted = statuses.get("accepted", 0)
        assert accepted >= 1200, statuses  # the threshold must have been crossed

        def listed_hot():
            r = http.get("/api/click-receiver/hot-ads")
            assert r.status_code == 200, r.text
            body = r.json()
            items = {i["ad_id"]: i for i in body["items"]}
            assert ad["id"] in items, f"ad {ad['id']} not hot yet (served by {r.headers.get('x-served-by')})"
            assert items[ad["id"]]["marks"] >= 1
            return body

        t_hot = time.monotonic()
        body = eventually(listed_hot, timeout=30)
        print(f"[flow 8] listed hot after {time.monotonic() - t_hot:.1f}s; threshold={body['threshold_clicks_10m']}")

        def next_click_hot():
            r = click_get(http, ad["id"], uid())
            assert r.status_code == 302, r.text
            if r.headers["x-click-status"] == "accepted":
                extra.append(1)
            assert r.headers["x-click-hot"] == "true"

        extra: list = []
        t_first = time.monotonic()
        eventually(next_click_hot, timeout=30)
        # all receivers learn it (flush 1 s + refresh 2 s): a burst of clicks spread over pods is all hot
        eventually(lambda: [next_click_hot() for _ in range(9)], timeout=30)
        print(f"[flow 8] every receiver answers hot {time.monotonic() - t_first:.1f}s after the first hot click")
        accepted += len(extra)

    frm, to = window(start)

    def counted():
        r = advertiser.ad_clicks(ad["id"], frm, to)
        assert r.status_code == 200, r.text
        assert r.json()["total"] == accepted, f"analytics {r.json()['total']} != accepted {accepted}"

    eventually(counted, timeout=90, interval=1)
