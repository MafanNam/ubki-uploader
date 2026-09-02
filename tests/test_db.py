"""File status aggregation over record statuses."""

import sqlite3

import pytest

from app import db
from app.db import FAILED, PARTIAL, PENDING, REJECTED, SENT


@pytest.mark.parametrize(
    "record_statuses,expected",
    [
        ([SENT, SENT], SENT),
        ([PENDING, PENDING], PENDING),
        ([SENT, PENDING], PARTIAL),
        ([SENT, FAILED], FAILED),
        ([PENDING, FAILED, REJECTED], FAILED),
        ([SENT, REJECTED], REJECTED),
        ([REJECTED], REJECTED),
        ([PENDING, REJECTED], PARTIAL),
    ],
)
def test_recompute_file_status(conn, record_statuses, expected):
    file_id = db.insert_file(conn, "a.jsonl", "f" * 64, 1, ["{}"] * len(record_statuses))
    for line_no, status in enumerate(record_statuses, start=1):
        conn.execute(
            "UPDATE records SET status = ? WHERE file_id = ? AND line_no = ?",
            (status, file_id, line_no),
        )
    conn.commit()
    assert db.recompute_file_status(conn, file_id) == expected


def test_recompute_zero_record_file_is_sent(conn):
    # a file of only blank lines ingests with zero records: nothing to send,
    # so it must complete (and get archived) instead of hanging as pending
    file_id = db.insert_file(conn, "empty.jsonl", "e" * 64, 1, [])
    assert db.recompute_file_status(conn, file_id) == SENT
    row = conn.execute("SELECT completed_at FROM files").fetchone()
    assert row["completed_at"] is not None


def test_completed_at_set_once_for_terminal_status(conn):
    file_id = db.insert_file(conn, "a.jsonl", "f" * 64, 1, ["{}"])
    conn.execute("UPDATE records SET status = ?", (SENT,))
    conn.commit()
    db.recompute_file_status(conn, file_id)
    first = conn.execute("SELECT completed_at FROM files").fetchone()["completed_at"]
    assert first is not None
    db.recompute_file_status(conn, file_id)
    assert conn.execute("SELECT completed_at FROM files").fetchone()["completed_at"] == first


def test_connect_migrates_a_db_created_before_warn_codes(tmp_path):
    """Prod databases predate the column and `CREATE TABLE IF NOT EXISTS` never
    touches an existing table, so `connect` must run the ADD COLUMN itself."""
    path = tmp_path / "legacy.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.execute(
        "CREATE TABLE records (id INTEGER PRIMARY KEY, uuid TEXT, file_id INTEGER,"
        " line_no INTEGER, raw_line TEXT, status TEXT, attempts INTEGER DEFAULT 0,"
        " last_error TEXT, ubki_response TEXT, created_at TEXT, sent_at TEXT)"
    )
    legacy.execute(
        "INSERT INTO records (id, status, attempts, created_at) VALUES (1, ?, 0, 'x')",
        (PENDING,),
    )
    legacy.commit()
    legacy.close()

    conn = db.connect(path)
    try:
        assert "warn_codes" in {row[1] for row in conn.execute("PRAGMA table_info(records)")}
        db.update_record_result(conn, 1, SENT, warn_codes="IG:3021")
        assert conn.execute("SELECT warn_codes FROM records").fetchone()[0] == "IG:3021"
    finally:
        conn.close()
