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


def seed_run(cfg, finished_at: str, status: str = "success") -> None:
    conn = db.connect(cfg.db_path)
    conn.execute(
        "INSERT INTO runs (started_at, finished_at, status) VALUES (?, ?, ?)",
        (finished_at, finished_at, status),
    )
    conn.commit()
    conn.close()


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


# --- health ----------------------------------------------------------------

def test_api_bootstraps_schema_on_empty_db(cfg):
    # get_conn opens connections with ensure_schema=False, so the factory must
    # bootstrap the schema once at startup — the API stays standalone-safe even
    # if it comes up before the uploader has ever created the DB.
    assert not cfg.db_path.exists()
    app = create_app(cfg)
    assert cfg.db_path.exists()  # created by the factory bootstrap
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "degraded"  # no successful run yet


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


def test_health_degraded_on_a_high_share_of_recent_rejections(api, cfg):
    seed_run(cfg, iso(datetime.now(timezone.utc)))
    seed_file(cfg, [SENT, REJECTED])
    body = api.get("/health").json()
    assert body["status"] == "degraded"
    assert any("were rejected (50.0% > 2.0%)" in reason for reason in body["reasons"])
    assert body["recent_rejections"] == {"window_days": 14, "rejected": 1, "total": 2,
                                         "ratio": 0.5, "max_ratio": 0.02}


def test_health_ok_when_recent_rejections_stay_under_the_threshold(api, cfg):
    # the everyday case: every producer file carries ~0.3% bureau rejections
    seed_run(cfg, iso(datetime.now(timezone.utc)))
    seed_file(cfg, [REJECTED] + [SENT] * 99)
    body = api.get("/health").json()
    assert body["status"] == "ok", body["reasons"]
    assert body["recent_rejections"]["ratio"] == 0.01
    assert body["record_counts"] == {SENT: 99, REJECTED: 1}   # still visible


def test_health_ignores_rejections_of_files_outside_the_window(api, cfg):
    # an old file resent later leaves its rejections behind as history
    seed_run(cfg, iso(datetime.now(timezone.utc)))
    file_id = seed_file(cfg, [REJECTED] * 10)
    conn = db.connect(cfg.db_path)
    old = iso(datetime.now(timezone.utc) - timedelta(days=cfg.health_rejected_window_days + 1))
    conn.execute("UPDATE files SET created_at = ? WHERE id = ?", (old, file_id))
    conn.commit()
    conn.close()
    body = api.get("/health").json()
    assert body["status"] == "ok", body["reasons"]
    assert body["recent_rejections"]["total"] == 0


def test_health_threshold_is_configurable(cfg):
    from dataclasses import replace

    seed_run(cfg, iso(datetime.now(timezone.utc)))
    seed_file(cfg, [REJECTED] + [SENT] * 99)
    with TestClient(create_app(replace(cfg, health_rejected_max_ratio=0.005))) as client:
        assert client.get("/health").json()["status"] == "degraded"


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


def test_health_ignores_aborted_runs(api, cfg):
    seed_run(cfg, iso(datetime.now(timezone.utc)), status="aborted")
    body = api.get("/health").json()
    assert body["status"] == "degraded"
    assert "no successful run yet" in body["reasons"]


# --- runs --------------------------------------------------------------------

def test_runs_listing_newest_first(api, cfg):
    seed_run(cfg, iso(datetime.now(timezone.utc) - timedelta(hours=2)))
    seed_run(cfg, iso(datetime.now(timezone.utc)), status="aborted")
    body = api.get("/runs").json()
    assert [r["status"] for r in body["runs"]] == ["aborted", "success"]

    assert len(api.get("/runs", params={"limit": 1}).json()["runs"]) == 1
    assert api.get("/runs", params={"limit": 0}).status_code == 422
    assert api.get("/runs", params={"limit": 500}).status_code == 422


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


def test_file_details_presents_ubki_response_as_json(api, cfg):
    file_id = seed_file(cfg, [SENT, FAILED])
    conn = db.connect(cfg.db_path)
    conn.execute(
        "UPDATE records SET ubki_response = ? WHERE file_id = ? AND line_no = 1",
        ('{"sentdatainfo":{"state":"ok","ok":1}}', file_id),
    )
    conn.execute(
        "UPDATE records SET ubki_response = ? WHERE file_id = ? AND line_no = 2",
        ("<html>bad gateway</html>", file_id),
    )
    conn.commit()
    conn.close()

    records = api.get(f"/files/{file_id}").json()["records"]
    assert records[0]["ubki_response"] == {"sentdatainfo": {"state": "ok", "ok": 1}}
    assert records[1]["ubki_response"] == "<html>bad gateway</html>"  # non-JSON stays raw


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
