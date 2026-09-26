import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt import PyJWKSet
from sdl_common.contract import assert_implements_contract

from auth.main import TokenSigner, app, get_signer

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PRIVATE_PEM = KEY.private_bytes(
    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
).decode()
PUBLIC_PEM = (
    KEY.public_key()
    .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    .decode()
)
ISS, AUD, KID = "ad-aggregator-auth", "ad-aggregator", "test-kid"


@pytest.fixture
def signer():
    return TokenSigner(KEY, kid=KID, issuer=ISS, audience=AUD, ttl=3600)


@pytest.fixture
def client(signer):
    app.dependency_overrides[get_signer] = lambda: signer
    yield TestClient(app)
    app.dependency_overrides.clear()


def verify_with_jwks(client, token):
    """Verify exactly like the gateway: pick the JWK by the token's kid, check RS256 + iss + aud."""
    jwks = PyJWKSet.from_dict(client.get("/.well-known/jwks.json").json())
    kid = jwt.get_unverified_header(token)["kid"]
    key = next(k for k in jwks.keys if k.key_id == kid)
    return jwt.decode(token, key.key, algorithms=["RS256"], issuer=ISS, audience=AUD)


def test_matches_contract():
    assert_implements_contract(app, "auth")


def test_health():
    assert TestClient(app).get("/healthz").json() == {"status": "ok"}


def test_viewer_token(client):
    res = client.post("/token", json={"role": "viewer", "user_id": "u-1"})
    assert res.status_code == 200
    body = res.json()
    assert body["token_type"] == "Bearer"
    assert body["expires_in"] == 3600
    assert body["role"] == "viewer"
    assert body["user_id"] == "u-1"
    assert body["advertiser_id"] is None
    claims = verify_with_jwks(client, body["access_token"])
    assert claims["sub"] == "u-1"
    assert claims["role"] == "viewer"
    assert claims["advertiser_id"] == ""  # always present (string) so the gateway overwrites spoofed headers
    assert claims["exp"] - claims["iat"] == 3600
    assert jwt.get_unverified_header(body["access_token"]) == {"alg": "RS256", "kid": KID, "typ": "JWT"}


def test_viewer_token_ignores_advertiser_id(client):
    body = client.post("/token", json={"role": "viewer", "user_id": "u-1", "advertiser_id": 7}).json()
    assert body["advertiser_id"] is None
    assert verify_with_jwks(client, body["access_token"])["advertiser_id"] == ""


def test_advertiser_token(client):
    res = client.post("/token", json={"role": "advertiser", "advertiser_id": 42})
    assert res.status_code == 200
    body = res.json()
    assert body["role"] == "advertiser"
    assert body["advertiser_id"] == 42
    assert body["user_id"] is None
    claims = verify_with_jwks(client, body["access_token"])
    assert claims["sub"] == "advertiser:42"
    assert claims["role"] == "advertiser"
    assert claims["advertiser_id"] == "42"
    assert claims["iss"] == ISS and claims["aud"] == AUD


@pytest.mark.parametrize(
    "payload",
    [
        {"role": "viewer"},
        {"role": "viewer", "user_id": ""},
        {"role": "viewer", "user_id": "x" * 129},
        {"role": "advertiser"},
        {"role": "advertiser", "advertiser_id": 0},
        {"role": "advertiser", "user_id": "u-1"},
        {"role": "admin", "user_id": "u-1"},
        {},
    ],
)
def test_invalid_requests_422(client, payload):
    assert client.post("/token", json=payload).status_code == 422


def test_token_signed_by_other_key_fails_verification(client):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = TokenSigner(other, kid=KID, issuer=ISS, audience=AUD, ttl=60).issue(
        sub="advertiser:1", role="advertiser", advertiser_id=1
    )
    with pytest.raises(jwt.InvalidSignatureError):
        verify_with_jwks(client, forged)


def test_expired_token_rejected(client, signer):
    token = signer.issue(sub="u", role="viewer", advertiser_id=None, now=1_000_000)
    with pytest.raises(jwt.ExpiredSignatureError):
        verify_with_jwks(client, token)


def test_jwks_shape(client):
    keys = client.get("/.well-known/jwks.json").json()["keys"]
    assert len(keys) == 1
    assert {k: keys[0][k] for k in ("kty", "kid", "use", "alg", "e")} == {
        "kty": "RSA",
        "kid": KID,
        "use": "sig",
        "alg": "RS256",
        "e": "AQAB",
    }


def test_lifespan_loads_key_from_connection_env(monkeypatch):
    monkeypatch.setenv("JWT_PRIVATE_KEY_PEM", PRIVATE_PEM)
    monkeypatch.setenv("JWT_PUBLIC_KEY_PEM", PUBLIC_PEM)
    monkeypatch.setenv("JWT_KID", "k1")
    monkeypatch.setenv("JWT_ISSUER", ISS)
    monkeypatch.setenv("JWT_AUDIENCE", AUD)
    with TestClient(app) as c:
        assert c.get("/readyz").status_code == 200
        token = c.post("/token", json={"role": "advertiser", "advertiser_id": 3}).json()["access_token"]
        assert jwt.get_unverified_header(token)["kid"] == "k1"
        claims = jwt.decode(token, PUBLIC_PEM, algorithms=["RS256"], audience=AUD, issuer=ISS)
        assert claims["advertiser_id"] == "3"


def test_lifespan_escaped_newlines_pem(monkeypatch):
    monkeypatch.setenv("JWT_PRIVATE_KEY_PEM", PRIVATE_PEM.replace("\n", "\\n"))
    monkeypatch.setenv("JWT_KID", "k1")
    with TestClient(app) as c:
        assert c.get("/readyz").status_code == 200


def test_missing_key_not_ready_and_503(monkeypatch):
    for name in ("JWT_PRIVATE_KEY_PEM", "JWT_PUBLIC_KEY_PEM", "JWT_KID"):
        monkeypatch.delenv(name, raising=False)
    with TestClient(app) as c:
        assert c.get("/healthz").status_code == 200
        ready = c.get("/readyz")
        assert ready.status_code == 503
        assert "jwt-key" in ready.json()["checks"]
        assert c.post("/token", json={"role": "viewer", "user_id": "u"}).status_code == 503
        assert c.get("/.well-known/jwks.json").status_code == 503
