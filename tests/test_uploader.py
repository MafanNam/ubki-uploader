"""Integration of run_pass with a scripted fake client."""

import pytest

from app import db
from app.db import FAILED, PENDING, REJECTED, SENT
from app.ubki_client import UploadResult
from app.uploader import run_pass

from .conftest import (
    FakeClient,
    network_failed_result,
    rejected_result,
    sent_result,
    write_jsonl,
)

LINES = ['{"inn":"1"}', '{"inn":"2"}', '{"inn":"3"}']


def record_statuses(cfg):
    conn = db.connect(cfg.db_path)
    try:
        return [r["status"] for r in conn.execute("SELECT status FROM records ORDER BY line_no")]
    finally:
        conn.close()


def file_row(cfg):
    conn = db.connect(cfg.db_path)
    try:
        return conn.execute("SELECT * FROM files").fetchone()
    finally:
        conn.close()


def test_success_flow_sends_all_and_archives(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES)
    client = FakeClient([sent_result()])
    summary = run_pass(cfg, client=client)

    assert summary.records_sent == 3
    assert record_statuses(cfg) == [SENT, SENT, SENT]
    row = file_row(cfg)
    assert row["status"] == SENT
    assert row["archived_at"] is not None
    assert not (cfg.data_folder / "a.jsonl").exists()
    assert (cfg.archive_folder / "a.jsonl").exists()
    # reqidout must be the record uuid, body must be the raw line
    assert client.calls[0][0] == LINES[0]
    assert len(client.calls[0][1]) == 32


def test_rejected_line_archives_file_but_flags_it(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES)
    client = FakeClient([sent_result(), rejected_result(), sent_result()])
    summary = run_pass(cfg, client=client)

    assert summary.records_sent == 2
    assert summary.records_rejected == 1
    assert record_statuses(cfg) == [SENT, REJECTED, SENT]
    row = file_row(cfg)
    assert row["status"] == REJECTED
    assert row["archived_at"] is not None  # terminal states only -> archived


def test_network_failure_keeps_file_for_retry(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES[:1])
    summary = run_pass(cfg, client=FakeClient([network_failed_result()]))

    assert summary.records_failed == 1
    assert record_statuses(cfg) == [FAILED]
    row = file_row(cfg)
    assert row["status"] == FAILED
    assert row["archived_at"] is None
    assert (cfg.data_folder / "a.jsonl").exists()


def test_failed_records_retried_next_pass_until_cap(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES[:1])
    for _ in range(cfg.retry_cap):
        run_pass(cfg, client=FakeClient([network_failed_result()]))

    conn = db.connect(cfg.db_path)
    record = conn.execute("SELECT * FROM records").fetchone()
    conn.close()
    assert record["attempts"] == cfg.retry_cap
    assert record["status"] == FAILED

    # over the cap: nothing is sent anymore
    client = FakeClient([sent_result()])
    run_pass(cfg, client=client)
    assert client.calls == []


def test_abort_after_consecutive_network_errors(cfg):
    lines = [f'{{"inn":"{i}"}}' for i in range(5)]
    write_jsonl(cfg.data_folder, "a.jsonl", lines)
    client = FakeClient([network_failed_result()])
    summary = run_pass(cfg, client=client)

    assert summary.aborted is True
    assert len(client.calls) == cfg.network_abort_threshold
    statuses = record_statuses(cfg)
    assert statuses.count(FAILED) == cfg.network_abort_threshold
    assert statuses.count(PENDING) == 5 - cfg.network_abort_threshold


def test_recovery_after_abort_sends_pending_and_failed(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES)
    run_pass(cfg, client=FakeClient([network_failed_result()]))
    summary = run_pass(cfg, client=FakeClient([sent_result()]))

    assert summary.records_sent == 3
    assert record_statuses(cfg) == [SENT, SENT, SENT]


def test_aborted_pass_is_not_a_successful_run(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES)
    run_pass(cfg, client=FakeClient([network_failed_result()]))

    conn = db.connect(cfg.db_path)
    try:
        run = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
        assert run["status"] == "aborted"
        assert "consecutive network errors" in run["error"]
        # health's 25h rule must keep working through a UBKI outage
        assert db.last_successful_run(conn) is None
    finally:
        conn.close()


def test_persistent_session_rejection_aborts_pass(cfg):
    """'session rejected twice' is network-like: no per-record auth storm."""
    lines = [f'{{"inn":"{i}"}}' for i in range(5)]
    write_jsonl(cfg.data_folder, "a.jsonl", lines)
    stale = UploadResult(status=FAILED, error="session rejected twice", is_network_error=True)
    client = FakeClient([stale])
    summary = run_pass(cfg, client=client)

    assert summary.aborted is True
    assert len(client.calls) == cfg.network_abort_threshold


def test_empty_file_marked_sent_and_archived(cfg):
    write_jsonl(cfg.data_folder, "empty.jsonl", ["", "  "])
    client = FakeClient([sent_result()])
    run_pass(cfg, client=client)

    assert client.calls == []
    row = file_row(cfg)
    assert row["lines_total"] == 0
    assert row["status"] == SENT
    assert row["archived_at"] is not None
    assert (cfg.archive_folder / "empty.jsonl").exists()


def test_line_under_limit_but_envelope_over_is_rejected(cfg):
    # the raw line squeaks under 2 MiB, but the envelope pushes the request over
    line = '{"x":"' + "a" * (cfg.max_line_bytes - 20) + '"}'
    write_jsonl(cfg.data_folder, "a.jsonl", [line])
    client = FakeClient([sent_result()])
    summary = run_pass(cfg, client=client)

    assert client.calls == []
    assert summary.records_rejected == 1
    assert record_statuses(cfg) == [REJECTED]


def test_oversized_line_rejected_locally(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", ['{"x":"' + "a" * (2 * 1024 * 1024) + '"}'])
    client = FakeClient([sent_result()])
    summary = run_pass(cfg, client=client)

    assert client.calls == []  # never sent
    assert summary.records_rejected == 1
    assert record_statuses(cfg) == [REJECTED]


def test_nt_state_counts_as_sent_with_warning(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES[:1])
    nt = UploadResult(status=SENT, state="nt", http_status=200,
                      response_text='{"state":"nt"}', has_warnings=True)
    summary = run_pass(cfg, client=FakeClient([nt]))

    assert summary.records_sent == 1
    assert summary.records_warnings == 1
    assert record_statuses(cfg) == [SENT]


def test_dry_run_sends_and_moves_nothing(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES)
    client = FakeClient([sent_result()])
    summary = run_pass(cfg, client=client, dry_run=True)

    assert summary.dry_run is True
    assert summary.files_new == 1
    assert client.calls == []
    assert (cfg.data_folder / "a.jsonl").exists()
    assert record_statuses(cfg) == []


def test_manual_retry_resets_rejected_and_resends(cfg):
    write_jsonl(cfg.data_folder, "a.jsonl", LINES[:1])
    run_pass(cfg, client=FakeClient([rejected_result()]))
    assert record_statuses(cfg) == [REJECTED]

    conn = db.connect(cfg.db_path)
    record_id = conn.execute("SELECT id FROM records").fetchone()["id"]
    assert db.reset_records(conn, record_id=record_id) == 1
    conn.close()

    # file is already archived; resend must work from records.raw_line
    summary = run_pass(cfg, client=FakeClient([sent_result()]))
    assert summary.records_sent == 1
    assert record_statuses(cfg) == [SENT]


def test_seeded_session_is_used_by_next_pass(cfg):
    from app.set_session import seed_session
    from app.uploader import DbSessionStore

    seed_session(cfg, "MANUAL_SESS")
    conn = db.connect(cfg.db_path)
    try:
        assert DbSessionStore(conn).load() == "MANUAL_SESS"
    finally:
        conn.close()
