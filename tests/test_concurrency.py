"""Parallel send path: fan-out, single auth, bounded abort, one-time re-auth.

The `cfg` fixture pins concurrency to 1 for determinism; these tests override
`ubki_concurrency` explicitly to exercise the concurrent behaviour."""

import dataclasses
import threading

import httpx

from app import db
from app.db import FAILED, PENDING, SENT
from app.ubki_client import UbkiClient, UploadResult
from app.uploader import run_pass

from .conftest import FakeClient, network_failed_result, sent_result, write_jsonl

AUTH_OK = {"doc": {"auth": {"sessid": "SESS123"}}}


def statuses(cfg):
    conn = db.connect(cfg.db_path)
    try:
        return [r["status"] for r in conn.execute("SELECT status FROM records ORDER BY line_no")]
    finally:
        conn.close()


def test_all_records_sent_concurrently_with_single_auth(cfg):
    cfg8 = dataclasses.replace(cfg, ubki_concurrency=8)
    lines = [f'{{"inn":"{i}"}}' for i in range(50)]
    write_jsonl(cfg8.data_folder, "a.jsonl", lines)
    client = FakeClient([sent_result()])

    summary = run_pass(cfg8, client=client)

    assert summary.records_sent == 50
    assert statuses(cfg8) == [SENT] * 50
    assert len(client.calls) == 50
    # authenticated exactly once for the whole pass, not per record
    assert client.auth_calls == 1
    assert client.reauth_calls == 0


def test_real_client_concurrent_uses_one_session(cfg):
    """End-to-end thread-safety with the real UbkiClient + MockTransport: many
    concurrent uploads, all carrying the one session, authenticated once."""
    cfg8 = dataclasses.replace(cfg, ubki_concurrency=8)
    lines = [f'{{"inn":"{i}"}}' for i in range(40)]
    write_jsonl(cfg8.data_folder, "a.jsonl", lines)

    lock = threading.Lock()
    auth_count = 0
    seen_sessids: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal auth_count
        if request.url.path.endswith("/auth"):
            with lock:
                auth_count += 1
            return httpx.Response(200, json=AUTH_OK)
        with lock:
            seen_sessids.append(request.headers.get("SessId"))
        return httpx.Response(200, json={"sentdatainfo": {"state": "ok"}})

    client = UbkiClient(cfg8, transport=httpx.MockTransport(handler))
    summary = run_pass(cfg8, client=client)

    assert summary.records_sent == 40
    assert statuses(cfg8) == [SENT] * 40
    assert auth_count == 1
    assert set(seen_sessids) == {"SESS123"}
    assert len(seen_sessids) == 40


def test_abort_under_concurrency_stops_dispatch(cfg):
    """When UBKI is down, the abort must stop dispatching new sends: only a
    bounded window (not the whole backlog) is ever sent, and the rest stays
    pending for the next pass."""
    cfg4 = dataclasses.replace(cfg, ubki_concurrency=4)
    lines = [f'{{"inn":"{i}"}}' for i in range(100)]
    write_jsonl(cfg4.data_folder, "a.jsonl", lines)
    client = FakeClient([network_failed_result()])

    summary = run_pass(cfg4, client=client)

    assert summary.aborted is True
    # far fewer than the full backlog were dispatched before the abort stopped it
    assert len(client.calls) < len(lines)
    st = statuses(cfg4)
    assert st.count(PENDING) > 0            # most records survive for the retry
    assert st.count(FAILED) == len(client.calls)


def test_reauth_once_then_resend_recovers_stale_session(cfg):
    """A mid-pass stale session (cold path) triggers exactly one re-auth on the
    main thread, then the affected record is resent and succeeds."""
    expired = UploadResult(status=FAILED, is_network_error=True,
                           session_expired=True, error="stale")
    write_jsonl(cfg.data_folder, "a.jsonl", ['{"inn":"1"}'])
    # first send returns session_expired, every send after re-auth returns ok
    client = FakeClient([expired, sent_result()])

    summary = run_pass(cfg, client=client)

    assert summary.records_sent == 1
    assert summary.records_failed == 0      # the stale-first attempt is not double-counted
    assert statuses(cfg) == [SENT]
    assert client.auth_calls == 1
    assert client.reauth_calls == 1


def test_reauth_failure_leaves_record_failed_and_retryable(cfg):
    """If the mid-pass re-auth itself fails, the stale record stays FAILED with
    attempts unburned (retried next pass) and the failure is surfaced."""
    expired = UploadResult(status=FAILED, is_network_error=True,
                           session_expired=True, error="stale")

    class ReauthFails(FakeClient):
        def reauth(self):
            self.reauth_calls += 1
            from app.ubki_client import UbkiAuthError
            raise UbkiAuthError("cannot reauth")

    write_jsonl(cfg.data_folder, "a.jsonl", ['{"inn":"1"}'])
    client = ReauthFails([expired])
    summary = run_pass(cfg, client=client)

    assert summary.records_failed == 1
    assert statuses(cfg) == [FAILED]
    assert any("re-auth failed" in e for e in summary.errors)
    conn = db.connect(cfg.db_path)
    try:
        attempts = conn.execute("SELECT attempts FROM records").fetchone()["attempts"]
    finally:
        conn.close()
    assert attempts == 0                     # network-like: attempts not burned
