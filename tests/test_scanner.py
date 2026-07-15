"""Folder scanning and ingestion: mtime guard, archive/ exclusion, sha256 dedup."""

import os
import time

from app import db
from app.uploader import RunSummary, ingest_new_files, scan_folder

from .conftest import write_jsonl

LINE = '{"inn":"1234567890","deals":[]}'


def test_scan_skips_fresh_files(cfg):
    write_jsonl(cfg.data_folder, "old.jsonl", [LINE])
    write_jsonl(cfg.data_folder, "fresh.jsonl", [LINE], age_sec=0)
    assert [p.name for p in scan_folder(cfg)] == ["old.jsonl"]


def test_scan_ignores_archive_and_hidden(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", [LINE])
    write_jsonl(cfg.data_folder, ".hidden", [LINE])
    cfg.archive_folder.mkdir()
    write_jsonl(cfg.archive_folder, "done.jsonl", [LINE])
    assert [p.name for p in scan_folder(cfg)] == ["a.jsonl"]


def test_scan_ignores_files_outside_glob(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", [LINE])
    write_jsonl(cfg.data_folder, "junk.txt", [LINE])
    write_jsonl(cfg.data_folder, "notes.jsonl.bak", [LINE])
    assert [p.name for p in scan_folder(cfg)] == ["a.jsonl"]


def test_ingest_creates_records_and_skips_blank_lines(cfg, conn):
    path = write_jsonl(cfg.data_folder, "a.jsonl", [LINE, "", "  ", LINE])
    summary = RunSummary()
    ingest_new_files(conn, [path], summary, dry_run=False)
    assert summary.files_new == 1
    file_row = conn.execute("SELECT * FROM files").fetchone()
    assert file_row["lines_total"] == 2
    records = conn.execute("SELECT * FROM records ORDER BY line_no").fetchall()
    assert [r["line_no"] for r in records] == [1, 2]
    assert all(r["raw_line"] == LINE for r in records)
    assert len({r["uuid"] for r in records}) == 2


def test_ingest_dedups_same_filename_and_hash(cfg, conn):
    path = write_jsonl(cfg.data_folder, "a.jsonl", [LINE])
    summary = RunSummary()
    ingest_new_files(conn, [path], summary, dry_run=False)
    ingest_new_files(conn, [path], summary, dry_run=False)
    assert summary.files_new == 1
    assert conn.execute("SELECT COUNT(*) AS n FROM files").fetchone()["n"] == 1


def test_ingest_overwritten_file_is_new_identity(cfg, conn):
    path = write_jsonl(cfg.data_folder, "a.jsonl", [LINE])
    ingest_new_files(conn, [path], RunSummary(), dry_run=False)
    path = write_jsonl(cfg.data_folder, "a.jsonl", [LINE, LINE])  # same name, new content
    ingest_new_files(conn, [path], RunSummary(), dry_run=False)
    assert conn.execute("SELECT COUNT(*) AS n FROM files").fetchone()["n"] == 2


def test_ingest_strips_crlf_line_endings(cfg, conn):
    path = cfg.data_folder / "crlf.jsonl"
    path.write_bytes(LINE.encode() + b"\r\n" + LINE.encode() + b"\r\n")
    mtime = time.time() - 600
    os.utime(path, (mtime, mtime))
    ingest_new_files(conn, [path], RunSummary(), dry_run=False)
    records = conn.execute("SELECT raw_line FROM records").fetchall()
    assert [r["raw_line"] for r in records] == [LINE, LINE]


def test_dry_run_writes_nothing(cfg, conn):
    path = write_jsonl(cfg.data_folder, "a.jsonl", [LINE])
    ingest_new_files(conn, [path], RunSummary(), dry_run=True)
    assert conn.execute("SELECT COUNT(*) AS n FROM files").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"] == 0
