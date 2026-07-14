"""Seed the cached UBKI session manually (test/debug helper).

Usage: python -m app.set_session <sessid>

Writes the sessid into the `meta` table keyed by today's Kyiv date — exactly
what `DbSessionStore` reads, so the next pass skips auth and uses this session.
Useful when auth is impossible from the current IP (UBKI whitelist) but a
valid sessid was obtained elsewhere. Expires with the UBKI day (23:59:59 Kyiv).
"""

from __future__ import annotations

import sys

from . import db
from .config import Config, load_config
from .uploader import DbSessionStore


def seed_session(config: Config, sessid: str) -> None:
    conn = db.connect(config.db_path)
    try:
        DbSessionStore(conn).save(sessid)
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1 or not argv[0].strip():
        print("usage: python -m app.set_session <sessid>", file=sys.stderr)
        return 2
    seed_session(load_config(), argv[0].strip())
    print("sessid cached for today (Kyiv time); next pass will use it without auth")
    return 0


if __name__ == "__main__":
    sys.exit(main())
