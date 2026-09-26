"""Identity from gateway-verified JWT claims (design spec §4.2).

Envoy Gateway verifies the RS256 token on protected routes and forwards its claims as headers:
`sub` -> X-Auth-Sub, `role` -> X-Auth-Role, `advertiser_id` -> X-Auth-Advertiser-Id (decimal string,
"" for viewers). The gateway always overwrites client-supplied copies of these headers, so services
trust them and only do *authorization*:

    from sdl_common.auth import Identity, current_identity, advertiser_owner

    @app.get("/ads")
    async def list_ads(who: Identity = Depends(current_identity)): ...          # any role, else 401

    @app.get("/advertisers/{advertiser_id}")
    async def get_one(advertiser_id: int, who: Identity = Depends(advertiser_owner)): ...  # 401/403

Missing headers -> 401 (defence in depth: normally the gateway already answered 401).
Wrong role or another advertiser's id -> 403.
"""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Path, status

SUB_HEADER = "X-Auth-Sub"
ROLE_HEADER = "X-Auth-Role"
ADVERTISER_HEADER = "X-Auth-Advertiser-Id"

ROLE_VIEWER = "viewer"
ROLE_ADVERTISER = "advertiser"

# For `responses=` on routes guarded by these dependencies (keeps the OpenAPI output in sync with
# the contracts' 401/403 responses).
ERROR_SCHEMA = {
    "type": "object",
    "required": ["detail"],
    "properties": {"detail": {"type": "string"}},
}
UNAUTHORIZED = {
    401: {"description": "No verified identity", "content": {"application/json": {"schema": ERROR_SCHEMA}}}
}
FORBIDDEN = {
    403: {
        "description": "Not the owner / wrong role",
        "content": {"application/json": {"schema": ERROR_SCHEMA}},
    }
}


@dataclass(frozen=True)
class Identity:
    sub: str
    role: str
    advertiser_id: int | None  # None for viewers (header "" or absent)

    @property
    def is_advertiser(self) -> bool:
        return self.role == ROLE_ADVERTISER and self.advertiser_id is not None


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(status.HTTP_401_UNAUTHORIZED, detail=detail, headers={"WWW-Authenticate": "Bearer"})


async def current_identity(
    # include_in_schema=False: these headers are set by the gateway, not by API clients.
    x_auth_sub: Annotated[str | None, Header(alias=SUB_HEADER, include_in_schema=False)] = None,
    x_auth_role: Annotated[str | None, Header(alias=ROLE_HEADER, include_in_schema=False)] = None,
    x_auth_advertiser_id: Annotated[
        str | None, Header(alias=ADVERTISER_HEADER, include_in_schema=False)
    ] = None,
) -> Identity:
    """Any authenticated caller (viewer or advertiser). 401 when the gateway identity is missing."""
    sub = (x_auth_sub or "").strip()
    role = (x_auth_role or "").strip()
    if not sub or not role:
        raise _unauthorized("missing authenticated identity")
    advertiser_id: int | None = None
    raw = (x_auth_advertiser_id or "").strip()
    if raw:
        try:
            advertiser_id = int(raw)
        except ValueError:
            # a signed token with a non-numeric advertiser_id claim: authenticated, but never an owner
            advertiser_id = None
        else:
            if advertiser_id < 1:
                advertiser_id = None
    return Identity(sub=sub, role=role, advertiser_id=advertiser_id)


def ensure_owner(who: Identity, advertiser_id: int) -> None:
    """403 unless `who` is an advertiser token for exactly this advertiser."""
    if who.role != ROLE_ADVERTISER:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="advertiser token required")
    if who.advertiser_id != advertiser_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="not the owner of this advertiser")


async def advertiser_owner(
    advertiser_id: Annotated[int, Path(ge=1)],
    who: Annotated[Identity, Depends(current_identity)],
) -> Identity:
    """Dependency for routes under /advertisers/{advertiser_id}: 401 if anonymous, 403 if not the owner."""
    ensure_owner(who, advertiser_id)
    return who
