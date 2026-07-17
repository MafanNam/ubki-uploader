"""Read-only FastAPI facade over the uploader DB (+ token-guarded actions).

Run with: uvicorn "app.api:create_app" --factory
The DB reflects facts of transmission — no CRUD beyond retry status resets.
"""

from __future__ import annotations

import hmac
import json
import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from . import db
from .config import Config, load_config
from .jsonlog import setup_logging

STALE_RUN_AFTER = timedelta(hours=25)


def _maybe_json(text: str | None):
    """ubki_response is stored as raw text; present it as an object when it
    parses (truncated/non-JSON bodies stay as-is)."""
    if not text:
        return text
    try:
        return json.loads(text)
    except ValueError:
        return text


def create_app(config: Config | None = None) -> FastAPI:
    # same structured JSON logs as the scheduler passes — but a factory must
    # not stomp root handlers configured by the host (pytest, an embedder)
    if not logging.getLogger().handlers:
        setup_logging()
    config = config or load_config()
    app = FastAPI(title="ubki-uploader")

    def get_conn():
        conn = db.connect(config.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def require_token(x_api_token: str = Header(default="")) -> None:
        if not hmac.compare_digest(x_api_token, config.api_token):
            raise HTTPException(status_code=401, detail="invalid or missing X-API-Token")

    @app.get("/health")
    def health(conn=Depends(get_conn)):
        reasons = []
        folder_accessible = config.data_folder.is_dir()
        if not folder_accessible:
            reasons.append("data folder not accessible")

        last_run = db.last_successful_run(conn)
        if last_run is None:
            reasons.append("no successful run yet")
        else:
            age = datetime.now(timezone.utc) - datetime.fromisoformat(last_run)
            if age > STALE_RUN_AFTER:
                reasons.append(
                    f"last successful run is {age.total_seconds() / 3600:.1f}h old (> 25h)"
                )

        failed_over_cap = conn.execute(
            "SELECT COUNT(*) AS n FROM records WHERE status = ? AND attempts >= ?",
            (db.FAILED, config.retry_cap),
        ).fetchone()["n"]
        if failed_over_cap:
            reasons.append(f"{failed_over_cap} record(s) failed beyond retry cap")

        rejected = conn.execute(
            "SELECT COUNT(*) AS n FROM records WHERE status = ?", (db.REJECTED,)
        ).fetchone()["n"]
        if rejected:
            reasons.append(f"{rejected} rejected record(s) awaiting manual review")

        counts = {
            row["status"]: row["n"]
            for row in conn.execute("SELECT status, COUNT(*) AS n FROM records GROUP BY status")
        }
        return {
            "status": "degraded" if reasons else "ok",
            "reasons": reasons,
            "folder_accessible": folder_accessible,
            "last_successful_run": last_run,
            "last_sent_at": db.last_sent_at(conn),
            "record_counts": counts,
        }

    @app.get("/runs")
    def list_runs(
        limit: int = Query(default=20, ge=1, le=200),
        conn=Depends(get_conn),
    ):
        return {"runs": [dict(row) for row in db.recent_runs(conn, limit)]}

    @app.get("/files")
    def list_files(
        status: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        conn=Depends(get_conn),
    ):
        if status is not None and status not in db.FILE_STATUSES:
            raise HTTPException(status_code=422, detail=f"status must be one of {db.FILE_STATUSES}")
        where = "WHERE status = ?" if status else ""
        params = (status,) if status else ()
        rows = conn.execute(
            f"SELECT * FROM files {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
        return {"files": [dict(row) for row in rows]}

    @app.get("/files/{file_id}")
    def file_details(file_id: int, conn=Depends(get_conn)):
        row = conn.execute("SELECT * FROM files WHERE id = ?", (file_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="file not found")
        records = conn.execute(
            "SELECT id, uuid, line_no, status, attempts, last_error, ubki_response,"
            " created_at, sent_at FROM records WHERE file_id = ? ORDER BY line_no",
            (file_id,),
        ).fetchall()
        return {
            "file": dict(row),
            "records": [
                dict(r) | {"ubki_response": _maybe_json(r["ubki_response"])}
                for r in records
            ],
        }

    @app.post("/files/{file_id}/retry", dependencies=[Depends(require_token)])
    def retry_file(file_id: int, conn=Depends(get_conn)):
        if not conn.execute("SELECT 1 FROM files WHERE id = ?", (file_id,)).fetchone():
            raise HTTPException(status_code=404, detail="file not found")
        reset = db.reset_records(conn, file_id=file_id)
        db.recompute_file_status(conn, file_id)
        return {"reset_records": reset}

    @app.post("/records/{record_id}/retry", dependencies=[Depends(require_token)])
    def retry_record(record_id: int, conn=Depends(get_conn)):
        row = conn.execute("SELECT file_id FROM records WHERE id = ?", (record_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="record not found")
        reset = db.reset_records(conn, record_id=record_id)
        if not reset:
            raise HTTPException(status_code=409, detail="record is not failed/rejected")
        db.recompute_file_status(conn, row["file_id"])
        return {"reset_records": reset}

    @app.post("/run", dependencies=[Depends(require_token)], status_code=202)
    def force_run():
        # Detached pass; flock inside run_pass prevents overlap with cron.
        subprocess.Popen(
            [sys.executable, "-m", "app.run_once"],
            stdout=None, stderr=None, start_new_session=True,
        )
        return {"started": True}

    return app
