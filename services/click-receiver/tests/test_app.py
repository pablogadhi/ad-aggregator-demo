import asyncio
import json
import uuid

import jsonschema
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from sdl_common.contract import assert_implements_contract, find_repo_root

from click_receiver.main import app
from click_receiver.store import dedup_key

EVENT_SCHEMA = json.loads((find_repo_root() / "design/contracts/events/clicks.schema.json").read_text())


def metric(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def assert_valid_event(value: bytes) -> dict:
    event = json.loads(value)
    jsonschema.validate(event, EVENT_SCHEMA, format_checker=jsonschema.FormatChecker())
    return event


def test_health():
    assert TestClient(app).get("/healthz").json() == {"status": "ok"}


def test_matches_contract():
    assert_implements_contract(app, "click-receiver")


# -- accepted -------------------------------------------------------------------------------
def test_get_click_redirects_after_ack(client, h):
    res = client.get("/click/42", params={"user_id": "u1"})
    assert res.status_code == 302
    assert res.headers["location"] == "https://advertiser.example/landing"
    assert res.headers["x-click-status"] == "accepted"
    assert res.headers["x-click-hot"] == "false"
    click_id = res.headers["x-click-id"]
    uuid.UUID(click_id)

    [(topic, key, value)] = h.producer.messages
    assert topic == "clicks" and key == b"42"
    event = assert_valid_event(value)
    assert event["click_id"] == click_id
    assert (event["ad_id"], event["advertiser_id"], event["salt"], event["user_id"]) == (42, 7, 0, "u1")
    assert event["receiver"] == "pod-1@node-a"
    assert event["clicked_at"].endswith("Z") and len(event["clicked_at"]) == 24  # ms precision


def test_post_clicks_returns_json(client, h):
    res = client.post("/clicks", json={"ad_id": 43, "user_id": "u1"})
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "accepted" and body["ad_id"] == 43 and body["hot"] is False
    assert body["redirect_url"] == "https://advertiser.example/other"
    assert body["click_id"] == res.headers["x-click-id"]
    assert res.headers["x-click-status"] == "accepted" and res.headers["x-click-hot"] == "false"
    assert len(h.producer.messages) == 1


# -- dedup ----------------------------------------------------------------------------------
def test_duplicate_redirects_but_is_not_produced(client, h):
    first = client.get("/click/42", params={"user_id": "u1"})
    before = metric("clicks_total", status="duplicate", hot="false")
    dup = client.get("/click/42", params={"user_id": "u1"})
    assert dup.status_code == 302 and dup.headers["x-click-status"] == "duplicate"
    assert dup.headers["location"] == "https://advertiser.example/landing"
    assert dup.headers["x-click-id"] != first.headers["x-click-id"]  # the new request's id
    assert len(h.producer.messages) == 1
    assert metric("clicks_total", status="duplicate", hot="false") == before + 1

    other_user = client.post("/clicks", json={"ad_id": 42, "user_id": "u2"})
    assert other_user.json()["status"] == "accepted"
    other_ad = client.post("/clicks", json={"ad_id": 43, "user_id": "u1"})
    assert other_ad.json()["status"] == "accepted"
    assert len(h.producer.messages) == 3


def test_dedup_key_holds_click_id_with_ttl(client, h):
    res = client.get("/click/42", params={"user_id": "u9"})

    async def check():
        key = dedup_key(42, "u9")
        assert await h.redis.get(key) == res.headers["x-click-id"]
        assert 0 < await h.redis.ttl(key) <= 600

    asyncio.run(check())


def test_redis_error_fails_open(client, h):
    h.store.fail.add("claim")
    before = metric("click_dedup_failopen_total")
    for _ in range(2):  # without dedup, both are accepted
        res = client.get("/click/42", params={"user_id": "u1"})
        assert res.status_code == 302 and res.headers["x-click-status"] == "accepted"
    assert len(h.producer.messages) == 2
    assert metric("click_dedup_failopen_total") == before + 2


def test_slow_redis_fails_open_within_budget(client, h):
    h.store.claim_delay = 0.5  # >> REDIS_TIMEOUT_MS (50 ms)
    before = metric("click_dedup_failopen_total")
    res = client.post("/clicks", json={"ad_id": 42, "user_id": "u1"})
    assert res.json()["status"] == "accepted"
    assert res.elapsed.total_seconds() < 0.4
    assert metric("click_dedup_failopen_total") == before + 1


# -- produce failure -------------------------------------------------------------------------
def test_produce_failure_is_503_and_releases_dedup_key(client, h):
    h.producer.fail = RuntimeError("broker timeout")
    before = metric("clicks_total", status="rejected", hot="false")
    res = client.get("/click/42", params={"user_id": "u1"})
    assert res.status_code == 503
    assert "location" not in res.headers
    uuid.UUID(res.headers["x-click-id"])
    assert res.json()["detail"]
    assert metric("clicks_total", status="rejected", hot="false") == before + 1
    assert asyncio.run(h.redis.exists(dedup_key(42, "u1"))) == 0

    # the client's retry is a fresh click, not a duplicate of an unrecorded one
    h.producer.fail = None
    retry = client.get("/click/42", params={"user_id": "u1"})
    assert retry.status_code == 302 and retry.headers["x-click-status"] == "accepted"
    assert len(h.producer.messages) == 1


def test_produce_failure_post_is_503(client, h):
    h.producer.fail = RuntimeError("no ack")
    res = client.post("/clicks", json={"ad_id": 42, "user_id": "u1"})
    assert res.status_code == 503 and "x-click-id" in res.headers


# -- ad lookup -------------------------------------------------------------------------------
def test_unknown_and_inactive_ads_are_404(client, h):
    for ad_id in (999, 44):
        res = client.get(f"/click/{ad_id}", params={"user_id": "u1"})
        assert res.status_code == 404 and "x-click-id" in res.headers
        assert client.post("/clicks", json={"ad_id": ad_id, "user_id": "u1"}).status_code == 404
    assert h.producer.messages == []


def test_ad_lookup_failure_is_503(client, h):
    from click_receiver.ads import AdLookupError

    h.source.error = AdLookupError("both pools down")
    assert client.get("/click/42", params={"user_id": "u1"}).status_code == 503


def test_validation(client):
    assert client.get("/click/42").status_code == 422
    assert client.get("/click/0", params={"user_id": "u"}).status_code == 422
    assert client.get("/click/42", params={"user_id": "x" * 129}).status_code == 422
    assert client.post("/clicks", json={"ad_id": 42}).status_code == 422
    assert client.post("/clicks", json={"ad_id": 42, "user_id": ""}).status_code == 422


def test_accepted_clicks_feed_hot_counters_duplicates_do_not(client, h):
    client.get("/click/42", params={"user_id": "u1"})
    client.get("/click/42", params={"user_id": "u1"})  # duplicate
    client.get("/click/42", params={"user_id": "u2"})
    h.producer.fail = RuntimeError("x")
    client.get("/click/42", params={"user_id": "u3"})  # rejected
    assert h.hot._pending == {42: 2}


# -- /hot-ads + salting ------------------------------------------------------------------------
def test_hot_ads_empty(client):
    body = client.get("/hot-ads").json()
    assert body == {
        "refreshed_at": None,
        "threshold_clicks_10m": 1200,
        "salt_buckets": 12,
        "salting_enabled": True,
        "items": [],
    }


def make_hot(h, ad_id=42, clicks=1200):
    h.hot._pending[ad_id] = clicks
    asyncio.run(h.hot.flush())


def test_hot_ad_is_salted(client, h):
    make_hot(h)
    items = client.get("/hot-ads").json()["items"]
    assert [(i["ad_id"], i["marks"], i["permanent"]) for i in items] == [(42, 1, False)]
    salts = set()
    for n in range(60):
        res = client.get("/click/42", params={"user_id": f"user-{n}"})
        assert res.headers["x-click-hot"] == "true"
    for _, key, value in h.producer.messages:
        event = assert_valid_event(value)
        assert key.decode() == f"42#{event['salt']}"
        assert 0 <= event["salt"] < 12
        salts.add(event["salt"])
    assert len(salts) > 1
    # a non-hot ad is not salted
    client.get("/click/43", params={"user_id": "x"})
    assert h.producer.messages[-1][1] == b"43"
    assert metric("clicks_total", status="accepted", hot="true") >= 60


def test_salting_disabled_detects_but_does_not_salt(make_client):
    h, client = make_client(hot_salting_enabled=False)
    make_hot(h)
    res = client.post("/clicks", json={"ad_id": 42, "user_id": "u1"})
    assert res.headers["x-click-hot"] == "true" and res.json()["hot"] is True
    [(_, key, value)] = h.producer.messages
    assert key == b"42" and assert_valid_event(value)["salt"] == 0
    assert client.get("/hot-ads").json()["salting_enabled"] is False
