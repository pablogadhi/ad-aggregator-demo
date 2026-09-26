"""Storage for advertisers + ads (schema: migrations/0001_ads.sql = design/contracts/db/postgres.sql).

Read routing (why):
- Owner-scoped endpoints (/advertisers/{id}/...) and every write use the **primary**, so an
  advertiser always reads its own writes (create an ad, then list it).
- Display reads (`GET /ads`, `GET /ads/{id}`) are the high-volume path and go to the **replica**
  (ReadRouter: primary if the replica is down). `GET /ads/{id}` retries on the primary on a miss,
  like the click-receiver, so a just-created ad is not a 404 because of replica lag. The list may
  lag the primary by the replication delay (milliseconds on a healthy cluster).
"""

from typing import Any, Protocol

import psycopg
from sdl_common.pgread import ReadRouter
from sdl_common.postgres import Database

Row = dict[str, Any]

AD_COLUMNS = "id, advertiser_id, content, img_url, redirect_url, active, created_at, updated_at"


class AdRepo(Protocol):
    async def list_active(self, *, limit: int, after_id: int) -> list[Row]: ...
    async def get_active(self, ad_id: int) -> Row | None: ...
    async def create_advertiser(self, name: str) -> Row: ...
    async def get_advertiser(self, advertiser_id: int) -> Row | None: ...
    async def list_advertiser_ads(
        self, advertiser_id: int, *, include_inactive: bool
    ) -> list[Row] | None: ...
    async def create_ad(
        self, advertiser_id: int, *, content: str, img_url: str | None, redirect_url: str
    ) -> Row | None: ...
    async def update_ad(
        self,
        advertiser_id: int,
        ad_id: int,
        *,
        content: str,
        img_url: str | None,
        redirect_url: str,
        active: bool,
    ) -> Row | None: ...
    async def deactivate_ad(self, advertiser_id: int, ad_id: int) -> bool: ...


class PgAdRepo:
    def __init__(self, db: Database, reads: ReadRouter, *, primary_timeout: float = 5.0):
        self.db = db
        self.reads = reads
        self.primary_timeout = primary_timeout  # pool acquire timeout: fail fast (503) instead of 30 s

    def _primary(self):
        return self.db.primary.connection(timeout=self.primary_timeout)

    async def list_active(self, *, limit: int, after_id: int) -> list[Row]:
        async def q(conn: psycopg.AsyncConnection) -> list[Row]:
            cur = await conn.execute(
                f"SELECT {AD_COLUMNS} FROM ads WHERE active AND id > %s ORDER BY id LIMIT %s",
                (after_id, limit),
            )
            return await cur.fetchall()

        return await self.reads.run(q)

    async def get_active(self, ad_id: int) -> Row | None:
        async def q(conn: psycopg.AsyncConnection) -> Row | None:
            cur = await conn.execute(f"SELECT {AD_COLUMNS} FROM ads WHERE id = %s AND active", (ad_id,))
            return await cur.fetchone()

        row = await self.reads.run(q)
        if row is None and self.reads.has_replica:
            async with self._primary() as conn:  # replica lag: confirm the miss on the primary
                row = await q(conn)
        return row

    async def create_advertiser(self, name: str) -> Row:
        async with self._primary() as conn:
            cur = await conn.execute(
                "INSERT INTO advertisers (name) VALUES (%s) RETURNING id, name, created_at", (name,)
            )
            return await cur.fetchone()

    async def get_advertiser(self, advertiser_id: int) -> Row | None:
        async with self._primary() as conn:
            cur = await conn.execute(
                "SELECT id, name, created_at FROM advertisers WHERE id = %s", (advertiser_id,)
            )
            return await cur.fetchone()

    async def list_advertiser_ads(self, advertiser_id: int, *, include_inactive: bool) -> list[Row] | None:
        async with self._primary() as conn:
            cur = await conn.execute("SELECT 1 FROM advertisers WHERE id = %s", (advertiser_id,))
            if await cur.fetchone() is None:
                return None
            cur = await conn.execute(
                f"SELECT {AD_COLUMNS} FROM ads WHERE advertiser_id = %s AND (%s OR active) ORDER BY id",
                (advertiser_id, include_inactive),
            )
            return await cur.fetchall()

    async def create_ad(
        self, advertiser_id: int, *, content: str, img_url: str | None, redirect_url: str
    ) -> Row | None:
        # INSERT ... SELECT: one atomic statement that also checks the advertiser exists (None -> 404)
        async with self._primary() as conn:
            cur = await conn.execute(
                "INSERT INTO ads (advertiser_id, content, img_url, redirect_url) "
                "SELECT id, %s, %s, %s FROM advertisers WHERE id = %s "
                f"RETURNING {AD_COLUMNS}",
                (content, img_url, redirect_url, advertiser_id),
            )
            return await cur.fetchone()

    async def update_ad(
        self,
        advertiser_id: int,
        ad_id: int,
        *,
        content: str,
        img_url: str | None,
        redirect_url: str,
        active: bool,
    ) -> Row | None:
        async with self._primary() as conn:
            cur = await conn.execute(
                "UPDATE ads SET content = %s, img_url = %s, redirect_url = %s, active = %s, "
                "updated_at = now() "
                f"WHERE id = %s AND advertiser_id = %s RETURNING {AD_COLUMNS}",
                (content, img_url, redirect_url, active, ad_id, advertiser_id),
            )
            return await cur.fetchone()

    async def deactivate_ad(self, advertiser_id: int, ad_id: int) -> bool:
        # Idempotent soft delete: a second DELETE matches the row again (204) without touching updated_at.
        async with self._primary() as conn:
            cur = await conn.execute(
                "UPDATE ads SET active = false, "
                "updated_at = CASE WHEN active THEN now() ELSE updated_at END "
                "WHERE id = %s AND advertiser_id = %s RETURNING id",
                (ad_id, advertiser_id),
            )
            return await cur.fetchone() is not None
