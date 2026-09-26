"""Tiny SQL migration runner.

    python -m sdl_common.migrate <package.with.sql.files> [URL_ENV]   (URL_ENV defaults to POSTGRES_URL)

URL_ENV names the env var holding the DSN, e.g. ANALYTICS_DB_URL for a service whose database is
the component instance `analytics-db` (connection `analytics-db` -> ANALYTICS_DB_* env vars).

Applies `NNNN_name.sql` files shipped inside a Python package, in lexical order, each in its own
transaction, recording them in `schema_migrations`. A Postgres advisory lock serialises concurrent
runners, so it is safe as an init container on every replica.
"""

import logging
import os
import sys
import time
from importlib import resources

import psycopg

log = logging.getLogger("sdl.migrate")
LOCK_ID = 7_400_417  # arbitrary, shared by every lab service


def pending(conn: psycopg.Connection, files: list[str]) -> list[str]:
    applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
    return [f for f in files if f not in applied]


def migrate(package: str, url: str) -> list[str]:
    files = sorted(
        f.name for f in resources.files(package).iterdir() if f.name.endswith(".sql") and f.is_file()
    )
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            todo = pending(conn, files)
            for name in todo:
                sql = resources.files(package).joinpath(name).read_text()
                with conn.transaction():
                    conn.execute(sql)
                    conn.execute("INSERT INTO schema_migrations(version) VALUES (%s)", (name,))
                log.info("applied %s", name)
            return todo
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        return 2
    url_env = sys.argv[2] if len(sys.argv) == 3 else "POSTGRES_URL"
    url = os.environ.get(url_env)
    if not url:
        log.error("%s is not set (add the connection to deploy/values.yaml)", url_env)
        return 2
    # The DB may still be failing over / starting: retry for up to ~2 minutes.
    for attempt in range(40):
        try:
            applied = migrate(sys.argv[1], url)
            log.info("migrations up to date (%d applied now)", len(applied))
            return 0
        except psycopg.OperationalError as exc:
            log.warning("database not reachable (attempt %d): %s", attempt + 1, exc)
            time.sleep(3)
    return 1


if __name__ == "__main__":
    sys.exit(main())
