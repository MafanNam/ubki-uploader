from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from app import db
from app.config import Config
from app.db import FAILED, REJECTED, SENT
from app.ubki_client import UploadResult

OLD_ENOUGH = 600  # seconds past the 5-minute mtime guard


@pytest.fixture(autouse=True)
def _info_logging(caplog):
    # exercise structured-log paths in every test: reserved LogRecord keys in
    # `extra` (e.g. "filename") only blow up when the level is enabled
    import logging

    caplog.set_level(logging.INFO)


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    data_folder = tmp_path / "inbox"
    data_folder.mkdir()
    raw_folder = tmp_path / "raw"
    raw_folder.mkdir()
    return Config(
        data_folder=data_folder,
        ubki_login="login",
        ubki_password="password",
        ubki_upload_url="https://test.invalid/upload/data",
        ubki_auth_url="https://test.invalid/b2_api_xml/ubki/auth",
        db_path=tmp_path / "data" / "ubki.sqlite3",
        api_token="secret-token",
        min_file_age_sec=300,
        file_glob="*.jsonl",  # prod default is *.txt; tests use .jsonl names
        raw_folder=raw_folder,
    )


@pytest.fixture
def conn(cfg: Config):
    connection = db.connect(cfg.db_path)
    yield connection
    connection.close()


def write_jsonl(folder: Path, name: str, lines: list[str], age_sec: int = OLD_ENOUGH) -> Path:
    path = folder / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    mtime = time.time() - age_sec
    os.utime(path, (mtime, mtime))
    return path


class FakeClient:
    """Scripted UbkiClient stand-in: yields queued results, then repeats the last."""

    def __init__(self, results: list[UploadResult] | None = None):
        self.results = list(results or [UploadResult(status=SENT, state="ok")])
        self.calls: list[tuple[str, str]] = []
        self.closed = False

    def upload_record(self, raw_line: str, reqidout: str) -> UploadResult:
        self.calls.append((raw_line, reqidout))
        if len(self.results) > 1:
            return self.results.pop(0)
        return self.results[0]

    def close(self) -> None:
        self.closed = True


def sent_result() -> UploadResult:
    return UploadResult(status=SENT, state="ok", http_status=200, response_text='{"state":"ok"}')


def rejected_result() -> UploadResult:
    return UploadResult(
        status=REJECTED, state="er", http_status=200,
        response_text='{"state":"er"}', error="rejected by UBKI (state=er)",
    )


def network_failed_result() -> UploadResult:
    return UploadResult(status=FAILED, error="network: boom", is_network_error=True)
