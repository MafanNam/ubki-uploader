"""SQLite storage: schema + thin helpers. No ORM, WAL mode."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

# File/record statuses. A file's status is an aggregate over its records.
PENDING = "pending"
PARTIAL = "partial"
SENT = "sent"
FAILED = "failed"
REJECTED = "rejected"

FILE_STATUSES = (PENDING, PARTIAL, SENT, FAILED, REJECTED)
RECORD_STATUSES = (PENDING, SENT, FAILED, REJECTED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id           INTEGER PRIMARY KEY,
    filename     TEXT NOT NULL,
    sha256       TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    lines_total  INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    created_at   TEXT NOT NULL,
    completed_at TEXT,
    archived_at  TEXT,
    UNIQUE (filename, sha256)
);

CREATE TABLE IF NOT EXISTS records (
    id            INTEGER PRIMARY KEY,
    uuid          TEXT NOT NULL UNIQUE,
    file_id       INTEGER NOT NULL REFERENCES files(id),
    line_no       INTEGER NOT NULL,
    raw_line      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    ubki_response TEXT,
    created_at    TEXT NOT NULL,
    sent_at       TEXT
);
CREATE INDEX IF NOT EXISTS idx_records_file ON records(file_id);
CREATE INDEX IF NOT EXISTS idx_records_status ON records(status);

CREATE TABLE IF NOT EXISTS runs (
    id               INTEGER PRIMARY KEY,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    status           TEXT NOT NULL,          -- success | aborted | error
    files_seen       INTEGER NOT NULL DEFAULT 0,
    records_sent     INTEGER NOT NULL DEFAULT 0,
    records_failed   INTEGER NOT NULL DEFAULT 0,
    records_rejected INTEGER NOT NULL DEFAULT 0,
    error            TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enriched_files (
    id                INTEGER PRIMARY KEY,
    filename          TEXT NOT NULL,
    sha256            TEXT NOT NULL,
    lines_total       INTEGER NOT NULL,
    lines_enriched    INTEGER NOT NULL,
    lines_quarantined INTEGER NOT NULL,
    created_at        TEXT NOT NULL,
    UNIQUE (filename, sha256)
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: Path | str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# --- files ---------------------------------------------------------------

def get_file_by_identity(conn: sqlite3.Connection, filename: str, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM files WHERE filename = ? AND sha256 = ?", (filename, sha256)
    ).fetchone()


def insert_file(conn: sqlite3.Connection, filename: str, sha256: str, size_bytes: int, lines: list[str]) -> int:
    cur = conn.execute(
        "INSERT INTO files (filename, sha256, size_bytes, lines_total, status, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (filename, sha256, size_bytes, len(lines), PENDING, utcnow()),
    )
    file_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO records (uuid, file_id, line_no, raw_line, status, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [
            (uuid.uuid4().hex, file_id, line_no, raw_line, PENDING, utcnow())
            for line_no, raw_line in enumerate(lines, start=1)
        ],
    )
    conn.commit()
    return file_id


def recompute_file_status(conn: sqlite3.Connection, file_id: int) -> str:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM records WHERE file_id = ? GROUP BY status", (file_id,)
    ).fetchall()
    statuses = {row["status"] for row in rows}
    # empty set (a file ingested with zero records) counts as sent: nothing
    # to transmit means the file is trivially complete and can be archived
    if statuses <= {SENT}:
        status = SENT
    elif statuses == {PENDING}:
        status = PENDING
    elif FAILED in statuses:
        status = FAILED
    elif statuses <= {REJECTED, SENT}:
        status = REJECTED
    else:
        status = PARTIAL
    completed = None
    if status in (SENT, REJECTED):
        completed = utcnow()
    conn.execute(
        "UPDATE files SET status = ?, completed_at = COALESCE(completed_at, ?) WHERE id = ?",
        (status, completed, file_id),
    )
    conn.commit()
    return status


def mark_archived(conn: sqlite3.Connection, file_id: int) -> None:
    conn.execute("UPDATE files SET archived_at = ? WHERE id = ?", (utcnow(), file_id))
    conn.commit()


# --- records -------------------------------------------------------------

def sendable_records(conn: sqlite3.Connection, retry_cap: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT r.*, f.filename FROM records r JOIN files f ON f.id = r.file_id"
        " WHERE r.status = ? OR (r.status = ? AND r.attempts < ?)"
        " ORDER BY r.file_id, r.line_no",
        (PENDING, FAILED, retry_cap),
    ).fetchall()


def update_record_result(
    conn: sqlite3.Connection,
    record_id: int,
    status: str,
    *,
    last_error: str | None = None,
    ubki_response: str | None = None,
    count_attempt: bool = True,
) -> None:
    """count_attempt=False keeps `attempts` unchanged — used for network-like
    failures so an outage can't push a record over retry_cap."""
    sent_at = utcnow() if status == SENT else None
    conn.execute(
        "UPDATE records SET status = ?, attempts = attempts + ?, last_error = ?,"
        " ubki_response = ?, sent_at = ? WHERE id = ?",
        (status, int(count_attempt), last_error, ubki_response, sent_at, record_id),
    )
    conn.commit()


def reset_records(conn: sqlite3.Connection, *, file_id: int | None = None, record_id: int | None = None) -> int:
    """failed/rejected -> pending (manual retry). Returns affected count."""
    assert (file_id is None) != (record_id is None)
    where, param = ("file_id", file_id) if file_id is not None else ("id", record_id)
    cur = conn.execute(
        f"UPDATE records SET status = ?, attempts = 0, last_error = NULL WHERE {where} = ?"
        " AND status IN (?, ?)",
        (PENDING, param, FAILED, REJECTED),
    )
    conn.commit()
    return cur.rowcount


# --- enriched files (enricher state; identity = filename + sha256) --------

def get_enriched_by_identity(conn: sqlite3.Connection, filename: str, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM enriched_files WHERE filename = ? AND sha256 = ?", (filename, sha256)
    ).fetchone()


def insert_enriched_file(
    conn: sqlite3.Connection, filename: str, sha256: str,
    lines_total: int, lines_enriched: int, lines_quarantined: int,
) -> None:
    conn.execute(
        "INSERT INTO enriched_files (filename, sha256, lines_total, lines_enriched,"
        " lines_quarantined, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (filename, sha256, lines_total, lines_enriched, lines_quarantined, utcnow()),
    )
    conn.commit()


# --- runs / meta ---------------------------------------------------------

def insert_run(conn: sqlite3.Connection, started_at: str, status: str, summary: dict, error: str | None = None) -> None:
    conn.execute(
        "INSERT INTO runs (started_at, finished_at, status, files_seen, records_sent,"
        " records_failed, records_rejected, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            started_at,
            utcnow(),
            status,
            summary.get("files_seen", 0),
            summary.get("records_sent", 0),
            summary.get("records_failed", 0),
            summary.get("records_rejected", 0),
            error,
        ),
    )
    conn.commit()


def recent_runs(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()


def last_successful_run(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT finished_at FROM runs WHERE status = 'success' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row["finished_at"] if row else None


def last_sent_at(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT MAX(sent_at) AS ts FROM records").fetchone()
    return row["ts"] if row else None


def meta_get(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()
