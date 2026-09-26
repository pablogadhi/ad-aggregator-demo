from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sdl_common.contract import assert_implements_contract

from ad_placement.main import app, get_repo


class FakeRepo:
    """In-memory AdRepo with the same semantics as PgAdRepo."""

    def __init__(self):
        self.advertisers: dict[int, dict] = {}
        self.ads: dict[int, dict] = {}
        self._adv_seq = 0
        self._ad_seq = 0

    @staticmethod
    def _now():
        return datetime.now(UTC)

    async def list_active(self, *, limit, after_id):
        rows = sorted(
            (a for a in self.ads.values() if a["active"] and a["id"] > after_id), key=lambda a: a["id"]
        )
        return [dict(r) for r in rows[:limit]]

    async def get_active(self, ad_id):
        ad = self.ads.get(ad_id)
        return dict(ad) if ad and ad["active"] else None

    async def create_advertiser(self, name):
        self._adv_seq += 1
        row = {"id": self._adv_seq, "name": name, "created_at": self._now()}
        self.advertisers[row["id"]] = row
        return dict(row)

    async def get_advertiser(self, advertiser_id):
        row = self.advertisers.get(advertiser_id)
        return dict(row) if row else None

    async def list_advertiser_ads(self, advertiser_id, *, include_inactive):
        if advertiser_id not in self.advertisers:
            return None
        rows = [
            a
            for a in self.ads.values()
            if a["advertiser_id"] == advertiser_id and (include_inactive or a["active"])
        ]
        return [dict(r) for r in sorted(rows, key=lambda a: a["id"])]

    async def create_ad(self, advertiser_id, *, content, img_url, redirect_url):
        if advertiser_id not in self.advertisers:
            return None
        self._ad_seq += 1
        now = self._now()
        row = {
            "id": self._ad_seq,
            "advertiser_id": advertiser_id,
            "content": content,
            "img_url": img_url,
            "redirect_url": redirect_url,
            "active": True,
            "created_at": now,
            "updated_at": now,
        }
        self.ads[row["id"]] = row
        return dict(row)

    async def update_ad(self, advertiser_id, ad_id, *, content, img_url, redirect_url, active):
        ad = self.ads.get(ad_id)
        if not ad or ad["advertiser_id"] != advertiser_id:
            return None
        ad.update(
            content=content, img_url=img_url, redirect_url=redirect_url, active=active, updated_at=self._now()
        )
        return dict(ad)

    async def deactivate_ad(self, advertiser_id, ad_id):
        ad = self.ads.get(ad_id)
        if not ad or ad["advertiser_id"] != advertiser_id:
            return False
        ad["active"] = False
        return True


@pytest.fixture
def repo():
    return FakeRepo()


@pytest.fixture
def client(repo):
    app.dependency_overrides[get_repo] = lambda: repo
    yield TestClient(app)
    app.dependency_overrides.clear()


def viewer(user="u-1"):
    return {"X-Auth-Sub": user, "X-Auth-Role": "viewer", "X-Auth-Advertiser-Id": ""}


def advertiser(adv_id):
    return {
        "X-Auth-Sub": f"advertiser:{adv_id}",
        "X-Auth-Role": "advertiser",
        "X-Auth-Advertiser-Id": str(adv_id),
    }


AD = {
    "content": "Buy shoes",
    "img_url": "https://img.example.com/s.png",
    "redirect_url": "https://shoes.example.com",
}


def signup(client, name="Acme"):
    res = client.post("/advertisers", json={"name": name}, headers=viewer())
    assert res.status_code == 201
    return res.json()["id"]


def test_matches_contract():
    assert_implements_contract(app, "ad-placement")


def test_health():
    assert TestClient(app).get("/healthz").json() == {"status": "ok"}


# ---- authn / authz ----


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/ads"),
        ("GET", "/ads/1"),
        ("POST", "/advertisers"),
        ("GET", "/advertisers/1"),
        ("GET", "/advertisers/1/ads"),
        ("POST", "/advertisers/1/ads"),
        ("PUT", "/advertisers/1/ads/1"),
        ("DELETE", "/advertisers/1/ads/1"),
    ],
)
def test_missing_identity_401(client, method, path):
    res = client.request(method, path, json={})
    assert res.status_code == 401
    assert res.json()["detail"]


def test_role_without_sub_401(client):
    assert client.get("/ads", headers={"X-Auth-Role": "viewer"}).status_code == 401


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", "/advertisers/{id}", None),
        ("GET", "/advertisers/{id}/ads", None),
        ("POST", "/advertisers/{id}/ads", AD),
        ("PUT", "/advertisers/{id}/ads/1", {**AD, "active": True}),
        ("DELETE", "/advertisers/{id}/ads/1", None),
    ],
)
def test_owner_only_403(client, method, path, body):
    adv = signup(client)
    other = signup(client, "Other")
    url = path.format(id=adv)
    assert client.request(method, url, json=body, headers=viewer()).status_code == 403
    assert client.request(method, url, json=body, headers=advertiser(other)).status_code == 403
    # role says advertiser but id missing/garbage: never an owner
    bad = {"X-Auth-Sub": "x", "X-Auth-Role": "advertiser", "X-Auth-Advertiser-Id": "abc"}
    assert client.request(method, url, json=body, headers=bad).status_code == 403
    # a viewer token cannot become an owner by carrying the advertiser id
    spoof = {**viewer(), "X-Auth-Advertiser-Id": str(adv)}
    assert client.request(method, url, json=body, headers=spoof).status_code == 403


# ---- flows ----


def test_onboarding_flow(client):
    adv = signup(client)
    owner = advertiser(adv)
    got = client.get(f"/advertisers/{adv}", headers=owner)
    assert got.status_code == 200
    assert got.json()["name"] == "Acme"

    a1 = client.post(f"/advertisers/{adv}/ads", json=AD, headers=owner)
    assert a1.status_code == 201
    ad1 = a1.json()
    assert ad1["active"] is True
    assert ad1["advertiser_id"] == adv
    assert ad1["click_url"] == f"/api/click-receiver/click/{ad1['id']}"
    assert ad1["redirect_url"] == "https://shoes.example.com"  # stored verbatim, not normalised
    ad2 = client.post(f"/advertisers/{adv}/ads", json={**AD, "img_url": None}, headers=owner).json()
    assert ad2["img_url"] is None

    listed = client.get(f"/advertisers/{adv}/ads", headers=owner).json()["items"]
    assert [a["id"] for a in listed] == [ad1["id"], ad2["id"]]

    page = client.get("/ads", headers=viewer()).json()
    assert [a["id"] for a in page["items"]] == [ad1["id"], ad2["id"]]
    assert page["next_after_id"] is None
    assert all(a["click_url"].startswith("/api/click-receiver/click/") for a in page["items"])

    one = client.get(f"/ads/{ad1['id']}", headers=viewer())
    assert one.status_code == 200 and one.json()["id"] == ad1["id"]


def test_create_advertiser_validation(client):
    assert client.post("/advertisers", json={"name": ""}, headers=viewer()).status_code == 422
    assert client.post("/advertisers", json={"name": "x" * 201}, headers=viewer()).status_code == 422
    assert client.post("/advertisers", json={}, headers=viewer()).status_code == 422


def test_advertiser_can_sign_up_with_advertiser_token(client):
    assert client.post("/advertisers", json={"name": "A"}, headers=advertiser(99)).status_code == 201


@pytest.mark.parametrize(
    "body",
    [
        {**AD, "redirect_url": "not a url"},
        {**AD, "redirect_url": "/relative/path"},
        {**AD, "redirect_url": "ftp://example.com/x"},
        {**AD, "redirect_url": "javascript:alert(1)"},
        {**AD, "content": ""},
        {**AD, "content": "x" * 2001},
        {**AD, "img_url": "no-scheme.png"},
        {"content": "x"},
    ],
)
def test_create_ad_validation(client, body):
    adv = signup(client)
    assert client.post(f"/advertisers/{adv}/ads", json=body, headers=advertiser(adv)).status_code == 422


def test_owner_of_unknown_advertiser_gets_404(client):
    owner = advertiser(12345)  # auth mints tokens without checking the advertiser exists
    assert client.get("/advertisers/12345", headers=owner).status_code == 404
    assert client.get("/advertisers/12345/ads", headers=owner).status_code == 404
    assert client.post("/advertisers/12345/ads", json=AD, headers=owner).status_code == 404


def test_update_and_reactivate(client):
    adv = signup(client)
    owner = advertiser(adv)
    ad = client.post(f"/advertisers/{adv}/ads", json=AD, headers=owner).json()
    upd = {"content": "New", "img_url": None, "redirect_url": "http://new.example.com/x?y=1", "active": False}
    res = client.put(f"/advertisers/{adv}/ads/{ad['id']}", json=upd, headers=owner)
    assert res.status_code == 200
    assert res.json()["content"] == "New" and res.json()["active"] is False
    assert client.get(f"/ads/{ad['id']}", headers=viewer()).status_code == 404
    res = client.put(f"/advertisers/{adv}/ads/{ad['id']}", json={**upd, "active": True}, headers=owner)
    assert res.json()["active"] is True
    assert client.get(f"/ads/{ad['id']}", headers=viewer()).status_code == 200


def test_update_requires_all_fields(client):
    adv = signup(client)
    owner = advertiser(adv)
    ad = client.post(f"/advertisers/{adv}/ads", json=AD, headers=owner).json()
    assert client.put(f"/advertisers/{adv}/ads/{ad['id']}", json=AD, headers=owner).status_code == 422


def test_cannot_touch_other_advertisers_ad(client):
    a, b = signup(client, "A"), signup(client, "B")
    ad = client.post(f"/advertisers/{a}/ads", json=AD, headers=advertiser(a)).json()
    # B addresses A's ad under B's own path: owner check passes, the ad isn't B's -> 404
    assert (
        client.put(
            f"/advertisers/{b}/ads/{ad['id']}", json={**AD, "active": True}, headers=advertiser(b)
        ).status_code
        == 404
    )
    assert client.delete(f"/advertisers/{b}/ads/{ad['id']}", headers=advertiser(b)).status_code == 404
    assert client.get(f"/ads/{ad['id']}", headers=viewer()).status_code == 200


def test_soft_delete_is_idempotent(client, repo):
    adv = signup(client)
    owner = advertiser(adv)
    ad = client.post(f"/advertisers/{adv}/ads", json=AD, headers=owner).json()
    assert client.delete(f"/advertisers/{adv}/ads/{ad['id']}", headers=owner).status_code == 204
    assert client.delete(f"/advertisers/{adv}/ads/{ad['id']}", headers=owner).status_code == 204
    assert repo.ads[ad["id"]]["active"] is False  # row kept (soft delete)
    assert client.get("/ads", headers=viewer()).json()["items"] == []
    assert client.get(f"/ads/{ad['id']}", headers=viewer()).status_code == 404
    everything = client.get(f"/advertisers/{adv}/ads", headers=owner).json()["items"]
    assert [a["active"] for a in everything] == [False]
    active_only = client.get(f"/advertisers/{adv}/ads?include_inactive=false", headers=owner).json()["items"]
    assert active_only == []
    assert client.delete(f"/advertisers/{adv}/ads/999", headers=owner).status_code == 404


def test_keyset_pagination(client):
    adv = signup(client)
    owner = advertiser(adv)
    ids = [client.post(f"/advertisers/{adv}/ads", json=AD, headers=owner).json()["id"] for _ in range(5)]
    client.delete(f"/advertisers/{adv}/ads/{ids[1]}", headers=owner)  # inactive ads are skipped
    active = [i for i in ids if i != ids[1]]

    seen, after = [], None
    while True:
        params = {"limit": 2} | ({"after_id": after} if after is not None else {})
        page = client.get("/ads", params=params, headers=viewer()).json()
        seen += [a["id"] for a in page["items"]]
        after = page["next_after_id"]
        if after is None:
            break
        assert after == page["items"][-1]["id"]
    assert seen == active

    exact = client.get("/ads", params={"limit": 4}, headers=viewer()).json()
    assert len(exact["items"]) == 4 and exact["next_after_id"] is None  # exactly one full last page


@pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 201}, {"after_id": -1}])
def test_pagination_validation(client, params):
    assert client.get("/ads", params=params, headers=viewer()).status_code == 422


def test_custom_click_url_prefix(client, monkeypatch):
    from ad_placement import main

    monkeypatch.setattr(main.settings, "click_url_prefix", "/c/")
    adv = signup(client)
    ad = client.post(f"/advertisers/{adv}/ads", json=AD, headers=advertiser(adv)).json()
    assert ad["click_url"] == f"/c/{ad['id']}"


def test_db_unavailable_is_503(client, repo):
    from psycopg_pool import PoolTimeout

    async def boom(**kwargs):
        raise PoolTimeout("couldn't get a connection after 5.00 sec")

    repo.list_active = boom
    res = client.get("/ads", headers=viewer())
    assert res.status_code == 503
    assert res.json() == {"detail": "database unavailable"}
