"""CLI entrypoint: python -m app.enrich [--dry-run]"""

from __future__ import annotations

import argparse
import sys

from .config import load_config
from .enricher import run_enrich
from .jsonlog import setup_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enrich raw producer files for UBKI upload")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="scan and report only: no DB writes, no MySQL queries, no file moves",
    )
    args = parser.parse_args(argv)

    setup_logging()
    config = load_config()
    if config.raw_folder is None:
        print("RAW_FOLDER env variable is required for enrichment", file=sys.stderr)
        return 2
    if not args.dry_run:
        missing = [name for name, value in {
            "MYSQL_HOST": config.mysql_host,
            "MYSQL_USER": config.mysql_user,
            "MYSQL_PASSWORD": config.mysql_password,
            "MYSQL_DB": config.mysql_db,
        }.items() if not value]
        if missing:
            print(f"missing required env variables: {', '.join(missing)}", file=sys.stderr)
            return 2

    try:
        summary = run_enrich(config, dry_run=args.dry_run)
    except Exception:
        return 1
    return 0 if not summary.errors else 1


if __name__ == "__main__":
    sys.exit(main())
