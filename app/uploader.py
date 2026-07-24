"""One upload pass: scan folder -> ingest new files -> send records -> archive.

Guarded by an flock lock so cron and `POST /run` can never run concurrently.
"""

from __future__ import annotations

import fcntl
import hashlib
import itertools
import logging
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from sqlite3 import Connection

from . import db
from .alerts import send_telegram
from .config import Config
from .db import FAILED, REJECTED, SENT, utcnow
from .ratelimit import RateLimiter
from .ubki_client import (
    UbkiAuthError,
    UbkiClient,
    UploadResult,
    build_envelope,
    kyiv_today,
)

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
    files_skipped: int = 0  # present in the folder but outside FILE_GLOB
    files_empty: int = 0    # ingested with zero non-blank lines
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


def unique_target(folder: Path, name: str, sha256: str) -> Path:
    """`folder/name`, or a sha-prefixed variant when the name is already taken.
    Shared by the archiver and the enricher so the collision-suffix scheme stays
    identical across both."""
    target = folder / name
    if target.exists():
        target = folder / f"{sha256[:8]}_{name}"
    return target


def scan_folder(config: Config, summary=None, folder: Path | None = None) -> list[Path]:
    """Files matching file_glob in the folder (default: the uploader inbox),
    older than min_file_age_sec (a guard against half-written files).
    Subfolders (archive/, enriched/, ...) and hidden files are ignored;
    anything else outside the glob is logged, counted in the summary (so it
    reaches the alert) and never sent. `summary` only needs a `files_skipped`
    attribute — the enricher passes its own summary object."""
    folder = folder or config.data_folder
    if not folder.is_dir():
        raise FileNotFoundError(f"data folder not accessible: {folder}")
    cutoff = time.time() - config.min_file_age_sec
    eligible = []
    for path in sorted(folder.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        if not fnmatch(path.name, config.file_glob):
            log.warning(
                "file ignored: name does not match FILE_GLOB",
                extra={"event": "file_skipped_pattern", "file": path.name,
                       "glob": config.file_glob},
            )
            if summary is not None:
                summary.files_skipped += 1
            continue
        if path.stat().st_mtime <= cutoff:
            eligible.append(path)
    return eligible


def read_lines(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as fh:
        return [line.rstrip("\r\n") for line in fh if line.strip()]


def ingest_new_files(conn: Connection, paths: list[Path], summary: RunSummary, dry_run: bool) -> None:
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
        if not lines:
            # likely a truncated/blank producer export — completes as `sent`
            # (nothing to transmit) but must not pass unnoticed
            log.warning(
                "file has no data lines",
                extra={"event": "file_empty", "file": path.name, "sha256": sha},
            )
            summary.files_empty += 1
        if dry_run:
            continue
        db.insert_file(conn, path.name, sha, path.stat().st_size, lines)


def send_records(conn: Connection, config: Config, client: UbkiClient, summary: RunSummary) -> set[int]:
    """Send sendable records concurrently (up to `ubki_concurrency` workers,
    paced to `ubki_max_rps`) and return the set of file_ids touched so the caller
    recomputes just those (plus the still-active files).

    Thread ownership is strict: worker threads only build an envelope and do the
    stateless POST (`client.send_prepared`); this thread owns the session, every
    DB write, the summary, and the abort accounting. That keeps the single SQLite
    connection and `self._sessid` single-threaded, and honours UBKI's
    "authenticate once per day" rule (auth happens here, once, before fan-out)."""
    touched: set[int] = set()
    records = db.sendable_records(conn, config.retry_cap)
    if not records:
        log.info("nothing to send", extra={"event": "send_skip"})
        return touched
    log.info(
        "sending records",
        extra={"event": "send_start", "count": len(records),
               "concurrency": config.ubki_concurrency, "max_rps": config.ubki_max_rps},
    )

    # Auth once, on this thread, before any worker runs. A failure here means the
    # whole pass is blocked: abort so the health 25h rule keeps ticking.
    try:
        sessid = client.ensure_session()
    except UbkiAuthError as exc:
        summary.aborted = True
        summary.errors.append(f"auth failed, pass aborted: {exc}")
        log.error("pass aborted: cannot authenticate with UBKI",
                  extra={"event": "abort_auth", "error": str(exc)})
        return touched

    limiter = RateLimiter(config.ubki_max_rps)
    streak = 0  # consecutive network-like errors, in completion order

    def commit(record, result: UploadResult, *, count_summary: bool = True) -> None:
        touched.add(record["file_id"])
        # network-like failures don't burn attempts: retry_cap guards against
        # permanently-bad records, not against UBKI/transport outages.
        db.update_record_result(
            conn, record["id"], result.status,
            last_error=result.error, ubki_response=result.response_text,
            count_attempt=not result.is_network_error,
        )
        log.info(
            "record processed",
            extra={
                "event": "record_result", "record_id": record["id"],
                "file": record["filename"], "line_no": record["line_no"],
                "status": result.status, "state": result.state, "error": result.error,
            },
        )
        # count_summary=False for a stale-session record in the first wave: it is
        # written FAILED to the DB (crash-safe) but not tallied, because it will
        # be resent under a fresh session and counted by its final outcome then.
        if not count_summary:
            return
        if result.status == SENT:
            summary.records_sent += 1
            if result.has_warnings:
                summary.records_warnings += 1
        elif result.status == REJECTED:
            summary.records_rejected += 1
        else:
            summary.records_failed += 1

    def send_one(record) -> UploadResult:
        # Runs on a worker thread. Build the envelope lazily here (not up front)
        # so at most `ubki_concurrency` envelopes are alive at once — memory
        # stays bounded even for large files. UBKI's 2 MB limit covers the whole
        # request; a local reject makes no network call and skips the rate slot.
        envelope = build_envelope(record["raw_line"], record["uuid"])
        if len(envelope) > config.max_line_bytes:
            return UploadResult(
                status=REJECTED, is_local_reject=True,
                error=f"request envelope exceeds {config.max_line_bytes} bytes, not sent",
            )
        limiter.acquire()
        return client.send_prepared(envelope, sessid)

    def run_wave(wave, *, final: bool) -> tuple[list, bool]:
        """Bounded-concurrency send over `wave`. Keeps at most `ubki_concurrency`
        requests in flight, so an abort dispatches no more than ~concurrency
        extra sends before it stops submitting. Returns (session_expired records,
        aborted). Mutates the enclosing `streak`, DB, summary and `touched`.

        `final=False` (first wave): stale-session records are collected for a
        one-time re-auth+resend and left out of the summary tally. `final=True`
        (the resend wave): everything counts, since there is no further retry."""
        nonlocal streak
        expired: list = []
        aborted = False
        pending = iter(wave)
        with ThreadPoolExecutor(max_workers=config.ubki_concurrency) as ex:
            in_flight = {
                ex.submit(send_one, rec): rec
                for rec in itertools.islice(pending, config.ubki_concurrency)
            }
            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    record = in_flight.pop(fut)
                    result = fut.result()
                    resend = result.session_expired and not final
                    if resend:
                        expired.append(record)
                    commit(record, result, count_summary=not resend)
                    if result.is_network_error:
                        streak += 1
                        if streak >= config.network_abort_threshold:
                            aborted = True
                    elif not result.is_local_reject:
                        # a completed network exchange (sent or UBKI-rejected)
                        # proves UBKI is up; a local reject is neutral evidence.
                        streak = 0
                    # refill the window unless we've decided to abort: in-flight
                    # sends still drain and commit, we just stop dispatching new
                    # ones so remaining records stay pending for the next pass.
                    if not aborted:
                        nxt = next(pending, None)
                        if nxt is not None:
                            in_flight[ex.submit(send_one, nxt)] = nxt
        return expired, aborted

    expired, aborted = run_wave(records, final=False)

    # One-time mid-pass re-auth for stale-session records (cold path: the sessid
    # is valid until 23:59 Kyiv and the pass runs at 06:00, so this rarely fires).
    # Records were committed FAILED in the first wave (network-like, attempts not
    # burned); resend them once under a fresh session. UBKI forbids frequent auth,
    # so this happens at most once — anything still stale afterwards stays FAILED
    # and retries next pass.
    if expired and not aborted:
        try:
            sessid = client.reauth()
        except UbkiAuthError as exc:
            # can't refresh: the records stay FAILED in the DB (attempts not
            # burned) and retry next pass; tally them now since this wave won't.
            summary.records_failed += len(expired)
            summary.errors.append(f"re-auth failed: {exc}")
            log.error("re-auth failed mid-pass",
                      extra={"event": "reauth_failed", "error": str(exc)})
        else:
            log.info("re-authenticated mid-pass, resending stale-session records",
                     extra={"event": "reauth", "count": len(expired)})
            _, aborted_again = run_wave(expired, final=True)
            aborted = aborted or aborted_again

    if aborted:
        summary.aborted = True
        summary.errors.append(
            f"aborted after {config.network_abort_threshold} consecutive"
            " network-like errors (transport/5xx/sy/session)"
        )
        log.error("pass aborted: UBKI unreachable or erroring",
                  extra={"event": "abort_network"})
    return touched


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
        target = unique_target(config.archive_folder, row["filename"], row["sha256"])
        source.rename(target)
        db.mark_archived(conn, row["id"])
        summary.files_archived += 1
        log.info(
            "file archived",
            extra={"event": "file_archived", "file": row["filename"], "target": str(target)},
        )


def build_alert(summary: RunSummary) -> str | None:
    if not (summary.records_failed or summary.records_rejected or summary.records_warnings
            or summary.files_skipped or summary.files_empty or summary.errors):
        return None
    lines = ["UBKI uploader: проблеми за останній прохід"]
    if summary.records_failed:
        lines.append(f"failed: {summary.records_failed} (буде ретрай)")
    if summary.records_rejected:
        lines.append(f"rejected: {summary.records_rejected} (потрібен ручний розбір)")
    if summary.records_warnings:
        lines.append(f"прийнято з зауваженнями (nt/ig): {summary.records_warnings}")
    if summary.files_skipped:
        lines.append(f"файлів у папці поза маскою FILE_GLOB: {summary.files_skipped}")
    if summary.files_empty:
        lines.append(f"порожніх файлів (0 рядків даних): {summary.files_empty}")
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
        paths = scan_folder(config, summary)
        summary.files_seen = len(paths)
        log.info(
            "pass started",
            extra={"event": "pass_start", "files_seen": len(paths), "dry_run": summary.dry_run},
        )
        ingest_new_files(conn, paths, summary, summary.dry_run)
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
        touched = send_records(conn, config, client, summary)

        # Recompute the active files (not yet archived — covers files ingested
        # this pass, including zero-record ones, and any in-progress file) plus
        # the files whose records this pass touched. The latter is what catches
        # an ARCHIVED file whose record was manually retried and re-sent: it is
        # no longer in the active set but is in `touched`. Bounded by the active
        # backlog + this pass's work, not by every file ever ingested.
        recompute_ids = {
            row["id"] for row in
            conn.execute("SELECT id FROM files WHERE archived_at IS NULL").fetchall()
        }
        recompute_ids |= touched
        for file_id in recompute_ids:
            db.recompute_file_status(conn, file_id)
        archive_completed_files(conn, config, summary)

        # an aborted pass must not advance last_successful_run (health 25h rule)
        run_status = "aborted" if summary.aborted else "success"
        db.insert_run(conn, started_at, run_status, summary.as_dict(),
                      error="; ".join(summary.errors) or None)
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
