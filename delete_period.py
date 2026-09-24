#!/usr/bin/env python3
"""One-off sender for packages the daily uploader cannot express: reqtype `d`
(delete the periods carried in the package, dir.50) with a `delreason`
(dir.62; partners may use only 26-29), and — for experiments on the test
contour — plain `u` packages that must NOT be recorded in the uploader DB.

Why it exists: a closure is blocked by CRITICAL 3004 when the bureau already
holds a LATER period of the deal (see CLAUDE.md, "the closing period is also
capped from above"). For deals the old service kept reporting as open after
they had actually ended, the fix is to delete those erroneous later periods
(`delreason=27` «Угода видалена партнером через помилку передачі даних») and
then send the closure through the ordinary uploader.

This tool never writes to the uploader DB: the session is read from `meta`
read-only (or passed with --sessid) and it NEVER authenticates — UBKI forbids
frequent auth, and a fresh sessid is the uploader's job. Every response goes to
stdout and, with --log, to a JSONL file. Sending is sequential, one package per
input line (the bare fo_cki subject, exactly as in an enriched file).

The target host is taken from UBKI_URL; anything other than test.ubki.ua
requires --prod, so an experiment can never reach the production base by a
stale env.

    python3 delete_period.py --input d.txt --reqtype d --delreason 27            # test
    python3 delete_period.py --input d.txt --reqtype d --delreason 27 --prod     # prod
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import uuid
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_DB = "/data/ubki.sqlite3"
TEST_HOST = "test.ubki.ua"
PARTNER_DELREASONS = {"26", "27", "28", "29"}  # dir.62: the only codes a partner may send
REQREASON_TRANSMISSION = "0"


def build_envelope(raw_line: str, reqidout: str, reqtype: str, delreason: str | None) -> bytes:
    """Same byte-for-byte embedding as app.ubki_client.build_envelope, plus the
    reqtype/delreason the daily uploader never sends."""
    extra = f',"delreason":"{delreason}"' if delreason else ""
    return ('{"reqtype":"%s","reqidout":"%s","reqreason":"%s"%s,"data":{"fo_cki":%s}}'
            % (reqtype, reqidout, REQREASON_TRANSMISSION, extra, raw_line.strip())
            ).encode("utf-8")


def check_args(reqtype: str, delreason: str | None, url: str, prod: bool) -> str | None:
    """None when the combination is allowed, else the reason it is refused."""
    if reqtype == "d" and delreason not in PARTNER_DELREASONS:
        return f"reqtype=d needs --delreason in {sorted(PARTNER_DELREASONS)}"
    if reqtype == "u" and delreason:
        return "--delreason only makes sense with reqtype=d"
    host = urlparse(url).hostname or ""
    if host != TEST_HOST and not prod:
        return f"UBKI_URL points at {host!r}, not {TEST_HOST}: pass --prod to send there"
    if host == TEST_HOST and prod:
        return "--prod given but UBKI_URL is the test contour"
    return None


def package_dlrefs(line: str) -> list[str]:
    """dlrefs carried by a package — logged so a later step can tell which deals
    a successful deletion unlocked (read only for the log, never re-serialized)."""
    try:
        return [str(d.get("dlref")) for d in json.loads(line).get("deals") or []]
    except (ValueError, AttributeError):
        return []


def read_sessid(db_path: str) -> str | None:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT value FROM meta WHERE key='ubki_sessid'").fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def summarize_response(text: str | None) -> dict:
    try:
        data = json.loads(text or "")
    except ValueError:
        return {"raw": (text or "")[:500]}
    info = data.get("sentdatainfo") if isinstance(data, dict) else None
    if not isinstance(info, dict):
        return {"raw": (text or "")[:500]}
    reqinfo = data.get("reqinfo") if isinstance(data.get("reqinfo"), dict) else {}
    return {"state": info.get("state"), "main_errcode": info.get("main_errcode"),
            "counters": {k: info.get(k) for k in ("ok", "nt", "ig", "er", "sy")},
            "reqid": reqinfo.get("reqid"),
            "items": [{k: i.get(k) for k in ("errtype", "errcode", "tag", "msg")}
                      for i in info.get("items") or [] if isinstance(i, dict)]}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Send reqtype d/u packages outside the uploader",
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog=__doc__)
    ap.add_argument("--input", required=True, type=Path, help="bare fo_cki subjects, one per line")
    ap.add_argument("--reqtype", choices=("d", "u"), required=True)
    ap.add_argument("--delreason")
    ap.add_argument("--prod", action="store_true", help="allow a non-test UBKI_URL")
    ap.add_argument("--sessid", help="default: read-only from the uploader DB meta table")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--log", type=Path, help="append one JSON result per package")
    args = ap.parse_args(argv)

    from app.config import load_config
    from app.ubki_client import UbkiClient

    config = load_config()
    refused = check_args(args.reqtype, args.delreason, config.ubki_upload_url, args.prod)
    if refused:
        print(refused, file=sys.stderr)
        return 2
    sessid = args.sessid or read_sessid(args.db)
    if not sessid:
        print("no sessid (seed one with app.set_session or pass --sessid)", file=sys.stderr)
        return 2

    client = UbkiClient(config)   # no session store: this tool never authenticates
    lines = [ln for ln in args.input.read_text(encoding="utf-8").splitlines() if ln.strip()]
    log_f = args.log.open("a", encoding="utf-8") if args.log else None
    try:
        for n, line in enumerate(lines, 1):
            reqidout = uuid.uuid4().hex
            result = client.send_prepared(
                build_envelope(line, reqidout, args.reqtype, args.delreason), sessid)
            out = {"n": n, "reqtype": args.reqtype, "delreason": args.delreason,
                   "dlrefs": package_dlrefs(line),
                   "reqidout": reqidout, "http": result.http_status, "status": result.status,
                   "error": result.error, "session_expired": result.session_expired,
                   **summarize_response(result.response_text)}
            print(json.dumps(out, ensure_ascii=False))
            if log_f:
                log_f.write(json.dumps(out, ensure_ascii=False) + "\n")
                log_f.flush()
            if result.session_expired:
                print("session rejected: stopping (this tool never re-authenticates)",
                      file=sys.stderr)
                return 1
    finally:
        if log_f:
            log_f.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
