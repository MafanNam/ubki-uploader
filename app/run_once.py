"""CLI entrypoint: python -m app.run_once [--dry-run]"""

from __future__ import annotations

import argparse
import sys

from .config import load_config
from .jsonlog import setup_logging
from .uploader import run_pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one UBKI upload pass")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="scan and report only: no DB writes, no sending, no archiving",
    )
    args = parser.parse_args(argv)

    setup_logging()
    config = load_config()
    try:
        summary = run_pass(config, dry_run=args.dry_run)
    except Exception:
        return 1
    return 0 if not summary.errors else 1


if __name__ == "__main__":
    sys.exit(main())
