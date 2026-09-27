"""Flow 1 — auth at the edge (spec §4.2, §8.1): the gateway rejects missing/invalid tokens with 401;
the service rejects other advertisers' paths with 403, even with a spoofed identity header."""

import base64
import json

from conftest import Advertiser, bearer, token, uid

PATH = "/api/analytics/advertisers/{id}/clicks"


def test_no_token_is_401(http):
    r = http.get(PATH.format(id=1))
    assert r.status_code == 401, r.text


def test_bad_signature_is_401(http):
    good = token(http, role="advertiser", advertiser_id=1)
    head, payload, sig = good.split(".")
    # flip bytes in the signature: same header/claims, invalid RS256 signature
    raw = bytearray(base64.urlsafe_b64decode(sig + "=" * (-len(sig) % 4)))
    raw[0] ^= 0xFF
    raw[-1] ^= 0xFF
    bad_sig = base64.urlsafe_b64encode(bytes(raw)).rstrip(b"=").decode()
    r = http.get(PATH.format(id=1), headers=bearer(f"{head}.{payload}.{bad_sig}"))
    assert r.status_code == 401, r.text


def test_forged_claims_are_401(http):
    """Claims edited to another advertiser keep the old signature -> invalid."""
    good = token(http, role="advertiser", advertiser_id=1)
    head, payload, sig = good.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims["advertiser_id"] = "2"
    forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    r = http.get(PATH.format(id=2), headers=bearer(f"{head}.{forged}.{sig}"))
    assert r.status_code == 401, r.text


def test_garbage_token_is_401(http):
    r = http.get(PATH.format(id=1), headers=bearer("not-a-jwt"))
    assert r.status_code == 401, r.text


def test_other_advertisers_path_is_403(http):
    a, b = Advertiser(http), Advertiser(http)
    r = http.get(PATH.format(id=b.id), headers=a.headers)
    assert r.status_code == 403, r.text
    # and the owner gets through
    assert http.get(PATH.format(id=a.id), headers=a.headers).status_code == 200


def test_spoofed_identity_header_is_overwritten(http):
    a, b = Advertiser(http), Advertiser(http)
    spoof = {**a.headers, "X-Auth-Advertiser-Id": str(b.id), "X-Auth-Role": "advertiser", "X-Auth-Sub": f"advertiser:{b.id}"}
    r = http.get(PATH.format(id=b.id), headers=spoof)
    assert r.status_code == 403, r.text
    # same spoof on ad-placement's owner routes
    r = http.get(f"/api/ad-placement/advertisers/{b.id}/ads", headers=spoof)
    assert r.status_code == 403, r.text


def test_viewer_token_cannot_read_analytics(http):
    viewer = token(http, role="viewer", user_id=uid())
    spoof = {**bearer(viewer), "X-Auth-Advertiser-Id": "1", "X-Auth-Role": "advertiser"}
    r = http.get(PATH.format(id=1), headers=spoof)
    assert r.status_code == 403, r.text


def test_ad_placement_requires_token(http):
    assert http.get("/api/ad-placement/ads").status_code == 401
