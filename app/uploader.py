"""One upload pass: scan folder -> ingest new files -> send records -> archive.

Guarded by an flock lock so cron and `POST /run` can never run concurrently.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from sqlite3 import Connection

from . import db
from .alerts import send_telegram
from .config import Config
from .db import FAILED, REJECTED, SENT, utcnow
from .ubki_client import UbkiClient, kyiv_today

log = logging.getLogger("ubki.uploader")

SESSID_KEY = "ubki_sessid"
SESSID_DATE_KEY = "ubki_sessid_date"


class DbSessionStore:
    """sessid is valid until 23:59:59 Kyiv time; cache it for the day."""

    def __init__(self, conn: Connection):
        self._conn = conn

    def load(self) -> str | None:
        if db.meta_get(self._conn, SESSID_DATE_KEY) != kyiv_today():
            return None
        return db.meta_get(self._conn, SESSID_KEY)

    def save(self, sessid: str) -> None:
        db.meta_set(self._conn, SESSID_KEY, sessid)
        db.meta_set(self._conn, SESSID_DATE_KEY, kyiv_today())


@dataclass
class RunSummary:
    dry_run: bool = False
    files_seen: int = 0
    files_new: int = 0
    records_sent: int = 0
    records_failed: int = 0
    records_rejected: int = 0
    records_warnings: int = 0
    files_archived: int = 0
    aborted: bool = False
    skipped_lock: bool = False
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return self.__dict__ | {"errors": list(self.errors)}


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_folder(config: Config) -> list[Path]:
    """Plain files in the data folder older than min_file_age_sec (a guard
    against half-written files). archive/ and hidden files are ignored."""
    if not config.data_folder.is_dir():
        raise FileNotFoundError(f"data folder not accessible: {config.data_folder}")
    cutoff = time.time() - config.min_file_age_sec
    eligible = [
        path
        for path in sorted(config.data_folder.iterdir())
        if path.is_file() and not path.name.startswith(".") and path.stat().st_mtime <= cutoff
    ]
    return eligible


def read_lines(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as fh:
        return [line.rstrip("\n") for line in fh if line.strip()]


def ingest_new_files(conn: Connection, config: Config, paths: list[Path], summary: RunSummary, dry_run: bool) -> None:
    for path in paths:
        sha = sha256_of(path)
        if db.get_file_by_identity(conn, path.name, sha):
            continue  # already ingested (identity = filename + sha256)
        lines = read_lines(path)
        log.info(
            "new file discovered",
            extra={"event": "file_new", "file": path.name, "sha256": sha, "lines": len(lines)},
        )
        summary.files_new += 1
        if dry_run:
            continue
        db.insert_file(conn, path.name, sha, path.stat().st_size, lines)


def send_records(conn: Connection, config: Config, client: UbkiClient, summary: RunSummary) -> None:
    records = db.sendable_records(conn, config.retry_cap)
    if not records:
        log.info("nothing to send", extra={"event": "send_skip"})
        return
    log.info("sending records", extra={"event": "send_start", "count": len(records)})
    consecutive_network_errors = 0
    for record in records:
        raw_line = record["raw_line"]
        if len(raw_line.encode("utf-8")) > config.max_line_bytes:
            db.update_record_result(
                conn, record["id"], REJECTED,
                last_error=f"line exceeds {config.max_line_bytes} bytes, not sent",
            )
            summary.records_rejected += 1
            continue

        result = client.upload_record(raw_line, record["uuid"])
        db.update_record_result(
            conn, record["id"], result.status,
            last_error=result.error, ubki_response=result.response_text,
        )
        log.info(
            "record processed",
            extra={
                "event": "record_result", "record_id": record["id"],
                "file": record["filename"], "line_no": record["line_no"],
                "status": result.status, "state": result.state, "error": result.error,
            },
        )
        if result.status == SENT:
            summary.records_sent += 1
            if result.has_warnings:
                summary.records_warnings += 1
        elif result.status == REJECTED:
            summary.records_rejected += 1
        else:
            summary.records_failed += 1

        if result.is_network_error:
            consecutive_network_errors += 1
            if consecutive_network_errors >= config.network_abort_threshold:
                summary.aborted = True
                summary.errors.append(
                    f"aborted after {consecutive_network_errors} consecutive network errors"
                )
                log.error("pass aborted: UBKI unreachable", extra={"event": "abort_network"})
                return
        else:
            consecutive_network_errors = 0


def archive_completed_files(conn: Connection, config: Config, summary: RunSummary) -> None:
    """Files whose records are all terminal (sent/rejected) leave the inbox.
    Retries are served from records.raw_line, not from the file."""
    rows = conn.execute(
        "SELECT f.* FROM files f WHERE f.archived_at IS NULL AND NOT EXISTS ("
        "  SELECT 1 FROM records r WHERE r.file_id = f.id AND r.status NOT IN (?, ?))",
        (SENT, REJECTED),
    ).fetchall()
    for row in rows:
        source = config.data_folder / row["filename"]
        if not source.is_file() or sha256_of(source) != row["sha256"]:
            # already moved/overwritten by a newer version; just mark it
            db.mark_archived(conn, row["id"])
            continue
        config.archive_folder.mkdir(exist_ok=True)
        target = config.archive_folder / row["filename"]
        if target.exists():
            target = config.archive_folder / f"{row['sha256'][:8]}_{row['filename']}"
        source.rename(target)
        db.mark_archived(conn, row["id"])
        summary.files_archived += 1
        log.info(
            "file archived",
            extra={"event": "file_archived", "file": row["filename"], "target": str(target)},
        )


def build_alert(summary: RunSummary) -> str | None:
    if not (summary.records_failed or summary.records_rejected or summary.records_warnings or summary.errors):
        return None
    lines = ["UBKI uploader: проблеми за останній прохід"]
    if summary.records_failed:
        lines.append(f"failed: {summary.records_failed} (буде ретрай)")
    if summary.records_rejected:
        lines.append(f"rejected: {summary.records_rejected} (потрібен ручний розбір)")
    if summary.records_warnings:
        lines.append(f"прийнято з зауваженнями (nt): {summary.records_warnings}")
    lines.extend(summary.errors)
    lines.append(f"sent: {summary.records_sent}")
    return "\n".join(lines)


def run_pass(config: Config, client: UbkiClient | None = None, dry_run: bool = False) -> RunSummary:
    summary = RunSummary(dry_run=dry_run)
    config.lock_path.parent.mkdir(parents=True, exist_ok=True)
    with config.lock_path.open("w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log.warning("another pass is already running, exiting", extra={"event": "lock_busy"})
            summary.skipped_lock = True
            return summary
        return _run_locked(config, client, summary)


def _run_locked(config: Config, client: UbkiClient | None, summary: RunSummary) -> RunSummary:
    started_at = utcnow()
    conn = db.connect(config.db_path)
    own_client = client is None
    try:
        paths = scan_folder(config)
        summary.files_seen = len(paths)
        log.info(
            "pass started",
            extra={"event": "pass_start", "files_seen": len(paths), "dry_run": summary.dry_run},
        )
        ingest_new_files(conn, config, paths, summary, summary.dry_run)
        if summary.dry_run:
            pending = conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE status = 'pending'"
            ).fetchone()["n"]
            log.info(
                "dry-run finished",
                extra={"event": "pass_dry_run", "files_new": summary.files_new, "pending_in_db": pending},
            )
            return summary

        if client is None:
            client = UbkiClient(config, session_store=DbSessionStore(conn))
        send_records(conn, config, client, summary)

        for row in conn.execute("SELECT DISTINCT file_id FROM records").fetchall():
            db.recompute_file_status(conn, row["file_id"])
        archive_completed_files(conn, config, summary)

        db.insert_run(conn, started_at, "success", summary.as_dict())
        log.info("pass finished", extra={"event": "pass_done", **summary.as_dict()})
    except Exception as exc:
        summary.errors.append(str(exc))
        db.insert_run(conn, started_at, "error", summary.as_dict(), error=str(exc))
        log.exception("pass crashed", extra={"event": "pass_error"})
        send_telegram(config, f"UBKI uploader: прохід впав з помилкою\n{exc}")
        raise
    else:
        alert = build_alert(summary)
        if alert:
            send_telegram(config, alert)
    finally:
        if own_client and client is not None:
            client.close()
        conn.close()
    return summary
