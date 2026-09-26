from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sdl_common.contract import assert_implements_contract

from analytics.buckets import BucketRange, floor_to
from analytics.main import app, get_clock, get_repo
from analytics.repo import Series


def utc(*args):
    return datetime(*args, tzinfo=UTC)


NOW = utc(2026, 9, 26, 12, 34, 56)
ADV, OTHER_ADV = 7, 8


class FakeRepo:
    """click_counts in memory: (ad_id, advertiser_id, minute, count, updated_at), same semantics as SQL."""

    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []

    def _in(self, rng: BucketRange, **match):
        return [
            r
            for r in self.rows
            if rng.start <= r[2] < rng.end
            and all(r[{"ad_id": 0, "advertiser_id": 1}[k]] == v for k, v in match.items())
        ]

    @staticmethod
    def _series(rng, rows):
        s = Series()
        for ad_id, _, minute, count, updated in rows:
            b = floor_to(minute, rng.step)
            s.buckets[b] = s.buckets.get(b, 0) + count
            s.by_ad[ad_id] = s.by_ad.get(ad_id, 0) + count
            s.last_updated_at = max(filter(None, [s.last_updated_at, updated]))
        return s

    async def ad_series(self, advertiser_id, ad_id, rng):
        self.calls.append(("ad", advertiser_id, ad_id, rng))
        s = self._series(rng, self._in(rng, ad_id=ad_id, advertiser_id=advertiser_id))
        s.by_ad = {}
        return s

    async def advertiser_series(self, advertiser_id, rng):
        self.calls.append(("advertiser", advertiser_id, rng))
        return self._series(rng, self._in(rng, advertiser_id=advertiser_id))


ROWS = [
    # ad 1 (advertiser 7): 12:30 -> 3, 12:31 -> 4, 12:34 (current minute) -> 2
    (1, ADV, utc(2026, 9, 26, 12, 30), 3, utc(2026, 9, 26, 12, 30, 5)),
    (1, ADV, utc(2026, 9, 26, 12, 31), 4, utc(2026, 9, 26, 12, 31, 5)),
    (1, ADV, utc(2026, 9, 26, 12, 34), 2, utc(2026, 9, 26, 12, 34, 50)),
    # ad 2 (advertiser 7): 12:31 -> 3
    (2, ADV, utc(2026, 9, 26, 12, 31), 3, utc(2026, 9, 26, 12, 31, 6)),
    # ad 3 (advertiser 7): outside the default hour
    (3, ADV, utc(2026, 9, 26, 9, 0), 5, utc(2026, 9, 26, 9, 0, 5)),
    # ad 9 belongs to another advertiser
    (9, OTHER_ADV, utc(2026, 9, 26, 12, 31), 100, utc(2026, 9, 26, 12, 31, 7)),
]


@pytest.fixture
def repo():
    return FakeRepo(ROWS)


@pytest.fixture
def client(repo):
    app.dependency_overrides[get_repo] = lambda: repo
    app.dependency_overrides[get_clock] = lambda: lambda: NOW
    yield TestClient(app)
    app.dependency_overrides.clear()


def owner(adv=ADV):
    return {"X-Auth-Sub": f"advertiser:{adv}", "X-Auth-Role": "advertiser", "X-Auth-Advertiser-Id": str(adv)}


def test_matches_contract():
    assert_implements_contract(app, "analytics")


def test_health():
    assert TestClient(app).get("/healthz").json() == {"status": "ok"}


# ---- authz ----

PATHS = [f"/advertisers/{ADV}/clicks", f"/advertisers/{ADV}/ads/1/clicks"]


@pytest.mark.parametrize("path", PATHS)
def test_401_without_identity(client, path):
    assert client.get(path).status_code == 401


@pytest.mark.parametrize("path", PATHS)
def test_403_for_viewer_and_other_advertiser(client, path, repo):
    viewer = {"X-Auth-Sub": "u1", "X-Auth-Role": "viewer", "X-Auth-Advertiser-Id": ""}
    assert client.get(path, headers=viewer).status_code == 403
    assert client.get(path, headers=owner(OTHER_ADV)).status_code == 403
    assert client.get(path, headers={**viewer, "X-Auth-Advertiser-Id": str(ADV)}).status_code == 403
    assert repo.calls == []  # never queried


# ---- series ----


def test_ad_clicks_defaults(client):
    res = client.get(f"/advertisers/{ADV}/ads/1/clicks", headers=owner())
    assert res.status_code == 200
    body = res.json()
    assert body["advertiser_id"] == ADV and body["ad_id"] == 1
    assert body["granularity"] == "minute"
    assert body["from"] == "2026-09-26T11:34:00Z"
    assert body["to"] == "2026-09-26T12:35:00Z"
    assert len(body["points"]) == 61
    assert body["total"] == 9
    nonzero = {p["start"]: p["clicks"] for p in body["points"] if p["clicks"]}
    assert nonzero == {"2026-09-26T12:30:00Z": 3, "2026-09-26T12:31:00Z": 4, "2026-09-26T12:34:00Z": 2}
    assert body["points"][-1] == {
        "start": "2026-09-26T12:34:00Z",
        "clicks": 2,
    }  # still-filling bucket included
    assert body["freshness"]["last_updated_at"] == "2026-09-26T12:34:50Z"


def test_ad_of_another_advertiser_is_zero_series_not_404(client):
    body = client.get(f"/advertisers/{ADV}/ads/9/clicks", headers=owner()).json()
    assert body["total"] == 0
    assert all(p["clicks"] == 0 for p in body["points"]) and len(body["points"]) == 61
    assert body["freshness"]["last_updated_at"] is None


def test_advertiser_clicks_with_by_ad(client):
    body = client.get(f"/advertisers/{ADV}/clicks", headers=owner()).json()
    assert body["total"] == 12
    assert body["by_ad"] == [
        {"ad_id": 1, "clicks": 9},
        {"ad_id": 2, "clicks": 3},
    ]  # descending, ad 3 out of range
    assert {p["start"]: p["clicks"] for p in body["points"] if p["clicks"]} == {
        "2026-09-26T12:30:00Z": 3,
        "2026-09-26T12:31:00Z": 7,
        "2026-09-26T12:34:00Z": 2,
    }
    assert "ad_id" not in body


def test_by_ad_ties_sorted_by_ad_id_and_zero_excluded(client, repo):
    repo.rows = [
        (5, ADV, utc(2026, 9, 26, 12, 0), 2, NOW),
        (4, ADV, utc(2026, 9, 26, 12, 0), 2, NOW),
        (6, ADV, utc(2026, 9, 26, 12, 0), 0, NOW),
    ]
    body = client.get(f"/advertisers/{ADV}/clicks", headers=owner()).json()
    assert body["by_ad"] == [{"ad_id": 4, "clicks": 2}, {"ad_id": 5, "clicks": 2}]


def test_hour_rollup_total_equals_minute_total(client):
    q = {"from": "2026-09-26T09:00:00Z", "to": "2026-09-26T13:00:00Z"}
    minute = client.get(f"/advertisers/{ADV}/clicks", params=q, headers=owner()).json()
    hour = client.get(
        f"/advertisers/{ADV}/clicks", params={**q, "granularity": "hour"}, headers=owner()
    ).json()
    assert minute["total"] == hour["total"] == 17
    assert [p["start"] for p in hour["points"]] == [
        "2026-09-26T09:00:00Z",
        "2026-09-26T10:00:00Z",
        "2026-09-26T11:00:00Z",
        "2026-09-26T12:00:00Z",
    ]
    assert [p["clicks"] for p in hour["points"]] == [5, 0, 0, 12]


def test_day_granularity(client):
    body = client.get(
        f"/advertisers/{ADV}/ads/1/clicks", params={"granularity": "day"}, headers=owner()
    ).json()
    assert body["from"] == "2026-09-26T00:00:00Z" and body["to"] == "2026-09-27T00:00:00Z"
    assert body["points"] == [{"start": "2026-09-26T00:00:00Z", "clicks": 9}]


def test_explicit_range_truncated_and_rounded(client, repo):
    q = {"from": "2026-09-26T12:30:30+00:00", "to": "2026-09-26T12:31:10Z"}
    body = client.get(f"/advertisers/{ADV}/ads/1/clicks", params=q, headers=owner()).json()
    assert body["from"] == "2026-09-26T12:30:00Z" and body["to"] == "2026-09-26T12:32:00Z"
    assert [p["clicks"] for p in body["points"]] == [3, 4]
    _, _, _, rng = repo.calls[-1]
    assert (rng.start, rng.end) == (utc(2026, 9, 26, 12, 30), utc(2026, 9, 26, 12, 32))


def test_offset_timestamps_are_converted_to_utc(client):
    q = {"from": "2026-09-26T14:30:00+02:00", "to": "2026-09-26T14:32:00+02:00"}
    body = client.get(f"/advertisers/{ADV}/ads/1/clicks", params=q, headers=owner()).json()
    assert body["from"] == "2026-09-26T12:30:00Z" and body["to"] == "2026-09-26T12:32:00Z"
    assert body["total"] == 7


# ---- validation ----


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize(
    "params",
    [
        {"from": "2026-09-26T12:00:00Z", "to": "2026-09-26T12:00:00Z"},  # from == to
        {"from": "2026-09-26T13:00:00Z", "to": "2026-09-26T12:00:00Z"},  # from > to
        {"from": "2026-09-25T00:00:00Z", "to": "2026-09-26T00:01:00Z"},  # 1441 minutes
        {"from": "2026-01-01T00:00:00Z", "to": "2026-12-31T00:00:00Z", "granularity": "hour"},
        {"from": "2026-09-26T13:00:00Z"},  # from after the default to (= now)
    ],
)
def test_400(client, path, params, repo):
    res = client.get(path, params=params, headers=owner())
    assert res.status_code == 400
    assert res.json()["detail"]
    assert repo.calls == []


def test_exactly_1440_minutes_ok(client):
    q = {"from": "2026-09-25T00:00:00Z", "to": "2026-09-26T00:00:00Z"}
    body = client.get(f"/advertisers/{ADV}/clicks", params=q, headers=owner()).json()
    assert len(body["points"]) == 1440


@pytest.mark.parametrize(
    "params",
    [
        {"from": "2026-09-26T12:00:00"},  # no offset
        {"to": "2026-09-26T12:00:00"},
        {"from": "yesterday"},
        {"granularity": "week"},
    ],
)
def test_422(client, params):
    assert client.get(f"/advertisers/{ADV}/clicks", params=params, headers=owner()).status_code == 422


def test_max_buckets_setting(client, monkeypatch):
    from analytics import main

    monkeypatch.setattr(main.settings, "max_buckets", 10)
    assert client.get(f"/advertisers/{ADV}/clicks", headers=owner()).status_code == 400  # 61 > 10
    q = {"from": "2026-09-26T12:00:00Z", "to": "2026-09-26T12:10:00Z"}
    assert client.get(f"/advertisers/{ADV}/clicks", params=q, headers=owner()).status_code == 200


def test_db_unavailable_503(client, repo):
    import psycopg

    async def down(*a, **k):
        raise psycopg.OperationalError("connection refused")

    repo.advertiser_series = down
    res = client.get(f"/advertisers/{ADV}/clicks", headers=owner())
    assert res.status_code == 503 and res.json() == {"detail": "database unavailable"}


def test_default_clock_is_utc_now():
    now = get_clock()()
    assert now.tzinfo is not None and abs(now - datetime.now(UTC)) < timedelta(seconds=5)
