"""API facade: health logic, token auth, retry transitions."""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app import db
from app.api import create_app
from app.db import FAILED, PENDING, REJECTED, SENT


@pytest.fixture
def api(cfg):
    app = create_app(cfg)
    with TestClient(app) as client:
        yield client


def seed_file(cfg, statuses: list[str]) -> int:
    conn = db.connect(cfg.db_path)
    file_id = db.insert_file(conn, "a.jsonl", "0" * 64, 10, ['{"inn":"1"}'] * len(statuses))
    for line_no, status in enumerate(statuses, start=1):
        conn.execute(
            "UPDATE records SET status = ? WHERE file_id = ? AND line_no = ?",
            (status, file_id, line_no),
        )
    conn.commit()
    db.recompute_file_status(conn, file_id)
    conn.close()
    return file_id


def seed_run(cfg, finished_at: str) -> None:
    conn = db.connect(cfg.db_path)
    conn.execute(
        "INSERT INTO runs (started_at, finished_at, status) VALUES (?, ?, 'success')",
        (finished_at, finished_at),
    )
    conn.commit()
    conn.close()


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


# --- health ----------------------------------------------------------------

def test_health_degraded_without_any_run(api):
    body = api.get("/health").json()
    assert body["status"] == "degraded"
    assert "no successful run yet" in body["reasons"]


def test_health_ok_after_recent_run(api, cfg):
    seed_run(cfg, iso(datetime.now(timezone.utc)))
    body = api.get("/health").json()
    assert body["status"] == "ok"
    assert body["reasons"] == []


def test_health_degraded_when_run_older_than_25h(api, cfg):
    seed_run(cfg, iso(datetime.now(timezone.utc) - timedelta(hours=26)))
    body = api.get("/health").json()
    assert body["status"] == "degraded"


def test_health_degraded_on_rejected_records(api, cfg):
    seed_run(cfg, iso(datetime.now(timezone.utc)))
    seed_file(cfg, [SENT, REJECTED])
    body = api.get("/health").json()
    assert body["status"] == "degraded"
    assert any("rejected" in reason for reason in body["reasons"])


def test_health_degraded_on_failed_over_cap(api, cfg):
    seed_run(cfg, iso(datetime.now(timezone.utc)))
    file_id = seed_file(cfg, [FAILED])
    conn = db.connect(cfg.db_path)
    conn.execute("UPDATE records SET attempts = ? WHERE file_id = ?", (cfg.retry_cap, file_id))
    conn.commit()
    conn.close()
    body = api.get("/health").json()
    assert body["status"] == "degraded"
    assert any("retry cap" in reason for reason in body["reasons"])


# --- files -------------------------------------------------------------------

def test_files_listing_and_details(api, cfg):
    file_id = seed_file(cfg, [SENT, REJECTED])
    listing = api.get("/files", params={"status": "rejected"}).json()
    assert [f["id"] for f in listing["files"]] == [file_id]

    details = api.get(f"/files/{file_id}").json()
    assert details["file"]["status"] == REJECTED
    assert [r["status"] for r in details["records"]] == [SENT, REJECTED]

    assert api.get("/files/999").status_code == 404
    assert api.get("/files", params={"status": "nope"}).status_code == 422


# --- POST auth ----------------------------------------------------------------

@pytest.mark.parametrize("path", ["/files/1/retry", "/records/1/retry", "/run"])
def test_post_requires_token(api, path):
    assert api.post(path).status_code == 401
    assert api.post(path, headers={"X-API-Token": "wrong"}).status_code == 401


def test_file_retry_resets_failed_and_rejected(api, cfg):
    file_id = seed_file(cfg, [SENT, FAILED, REJECTED])
    resp = api.post(f"/files/{file_id}/retry", headers={"X-API-Token": cfg.api_token})
    assert resp.status_code == 200
    assert resp.json() == {"reset_records": 2}

    details = api.get(f"/files/{file_id}").json()
    statuses = [r["status"] for r in details["records"]]
    assert statuses == [SENT, PENDING, PENDING]
    assert details["file"]["status"] == "partial"


def test_record_retry_only_touches_one(api, cfg):
    file_id = seed_file(cfg, [REJECTED, REJECTED])
    details = api.get(f"/files/{file_id}").json()
    record_id = details["records"][0]["id"]

    resp = api.post(f"/records/{record_id}/retry", headers={"X-API-Token": cfg.api_token})
    assert resp.status_code == 200

    statuses = [r["status"] for r in api.get(f"/files/{file_id}").json()["records"]]
    assert statuses == [PENDING, REJECTED]


def test_record_retry_conflict_when_already_sent(api, cfg):
    file_id = seed_file(cfg, [SENT])
    record_id = api.get(f"/files/{file_id}").json()["records"][0]["id"]
    resp = api.post(f"/records/{record_id}/retry", headers={"X-API-Token": cfg.api_token})
    assert resp.status_code == 409
