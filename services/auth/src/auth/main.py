"""auth — implements design/contracts/openapi/auth.yaml (spec §5.2).

Demo token issuer: no passwords, it mints RS256 JWTs for a `viewer` (end user) or an `advertiser`.
Envoy Gateway verifies them on the ad-placement / analytics routes and forwards the claims as
X-Auth-* headers, so every token carries `sub`, `role` and `advertiser_id` as *strings*
(claimToHeaders needs all three to always overwrite client-supplied copies).

Key material comes from connection `jwt` (Secret apps/jwt-conn -> JWT_PRIVATE_KEY_PEM, JWT_KID,
JWT_ISSUER, JWT_AUDIENCE, JWT_PUBLIC_KEY_PEM). The JWKS is derived from the private key, so it can
never disagree with the signatures this service produces.
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Annotated, Literal

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from fastapi import Depends, FastAPI, HTTPException, Request
from jwt.algorithms import RSAAlgorithm
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sdl_common import ServiceSettings, create_app

log = logging.getLogger("auth")


class Settings(ServiceSettings):
    token_ttl_seconds: int = Field(default=3600, ge=60)


class JwtSettings(BaseSettings):
    """Connection contract `jwt` (env prefix JWT_)."""

    model_config = SettingsConfigDict(env_prefix="JWT_", extra="ignore")

    private_key_pem: str
    public_key_pem: str | None = None
    kid: str
    issuer: str = "ad-aggregator-auth"
    audience: str = "ad-aggregator"


settings = Settings(service_name="auth")


def _pem(value: str) -> bytes:
    # tolerate PEMs passed with literal "\n" (e.g. from a .env file); Secrets keep real newlines
    return value.replace("\\n", "\n").strip().encode()


class TokenSigner:
    def __init__(self, private_key: RSAPrivateKey, *, kid: str, issuer: str, audience: str, ttl: int):
        self.private_key = private_key
        self.public_key: RSAPublicKey = private_key.public_key()
        self.kid = kid
        self.issuer = issuer
        self.audience = audience
        self.ttl = ttl

    @classmethod
    def from_settings(cls, jwt_settings: JwtSettings, ttl: int) -> "TokenSigner":
        key = serialization.load_pem_private_key(_pem(jwt_settings.private_key_pem), password=None)
        if not isinstance(key, RSAPrivateKey):
            raise ValueError("JWT_PRIVATE_KEY_PEM is not an RSA private key")
        signer = cls(
            key, kid=jwt_settings.kid, issuer=jwt_settings.issuer, audience=jwt_settings.audience, ttl=ttl
        )
        if jwt_settings.public_key_pem:
            published = serialization.load_pem_public_key(_pem(jwt_settings.public_key_pem))
            if published.public_numbers() != signer.public_key.public_numbers():  # type: ignore[union-attr]
                # the gateway may verify with JWT_PUBLIC_KEY_PEM: a mismatch means every token is rejected
                log.error(
                    "JWT_PUBLIC_KEY_PEM does not match JWT_PRIVATE_KEY_PEM; JWKS uses the private key's"
                )
        return signer

    def issue(self, *, sub: str, role: str, advertiser_id: int | None, now: int | None = None) -> str:
        iat = int(time.time()) if now is None else now
        claims = {
            "sub": sub,
            "role": role,
            "advertiser_id": "" if advertiser_id is None else str(advertiser_id),
            "iss": self.issuer,
            "aud": self.audience,
            "iat": iat,
            "exp": iat + self.ttl,
        }
        return jwt.encode(claims, self.private_key, algorithm="RS256", headers={"kid": self.kid})

    def jwks(self) -> dict:
        jwk = RSAAlgorithm.to_jwk(self.public_key, as_dict=True)
        return {
            "keys": [
                {"kty": "RSA", "kid": self.kid, "use": "sig", "alg": "RS256", "n": jwk["n"], "e": jwk["e"]}
            ]
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    # A missing/invalid key must not crash-loop the pod silently: keep serving /healthz, fail /readyz
    # with the reason, and answer 503 on the API until the Secret is fixed (pod restart reloads it).
    app.state.signer = None
    app.state.signer_error = None
    try:
        app.state.signer = TokenSigner.from_settings(JwtSettings(), settings.token_ttl_seconds)
    except Exception as exc:  # noqa: BLE001
        app.state.signer_error = f"{type(exc).__name__}: {exc}"[:300]
        log.error("signing key not loaded", extra={"extra_fields": {"error": app.state.signer_error}})
    yield


async def _key_loaded() -> None:
    if app.state.signer is None:
        raise RuntimeError(app.state.signer_error or "signing key not loaded")


app = create_app(settings, title="auth", lifespan=lifespan, readiness=[("jwt-key", _key_loaded)])


def get_signer(request: Request) -> TokenSigner:
    signer = getattr(request.app.state, "signer", None)
    if signer is None:
        raise HTTPException(503, detail="signing key not available")
    return signer


# ---- schemas (contract: components.schemas) ----


class TokenRequest(BaseModel):
    role: Literal["viewer", "advertiser"]
    user_id: str | None = Field(default=None, min_length=1, max_length=128)
    advertiser_id: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _role_fields(self) -> "TokenRequest":
        if self.role == "viewer" and self.user_id is None:
            raise ValueError("user_id is required when role = viewer")
        if self.role == "advertiser" and self.advertiser_id is None:
            raise ValueError("advertiser_id is required when role = advertiser")
        return self


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["Bearer"] = "Bearer"
    expires_in: int
    role: Literal["viewer", "advertiser"]
    advertiser_id: int | None = None
    user_id: str | None = None


class Jwk(BaseModel):
    kty: Literal["RSA"]
    kid: str
    use: Literal["sig"]
    alg: Literal["RS256"]
    n: str
    e: str


class Jwks(BaseModel):
    keys: list[Jwk]


# ---- routes ----


@app.post("/token", response_model=TokenResponse, responses={422: {"description": "Invalid request"}})
async def issue_token(
    body: TokenRequest, signer: Annotated[TokenSigner, Depends(get_signer)]
) -> TokenResponse:
    if body.role == "viewer":
        # a viewer token never carries an advertiser id, even if one was sent
        token = signer.issue(sub=body.user_id, role="viewer", advertiser_id=None)
        return TokenResponse(access_token=token, expires_in=signer.ttl, role="viewer", user_id=body.user_id)
    token = signer.issue(
        sub=f"advertiser:{body.advertiser_id}", role="advertiser", advertiser_id=body.advertiser_id
    )
    return TokenResponse(
        access_token=token, expires_in=signer.ttl, role="advertiser", advertiser_id=body.advertiser_id
    )


@app.get("/.well-known/jwks.json", response_model=Jwks)
async def get_jwks(signer: Annotated[TokenSigner, Depends(get_signer)]) -> dict:
    return signer.jwks()
