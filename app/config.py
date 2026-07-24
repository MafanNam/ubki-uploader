"""Configuration loaded entirely from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_UPLOAD_URL = "https://secure.ubki.ua/upload/data"
DEFAULT_AUTH_URL = "https://secure.ubki.ua/b2_api_xml/ubki/auth"

KYIV_TZ = "Europe/Kyiv"


@dataclass(frozen=True)
class Config:
    data_folder: Path
    ubki_login: str
    ubki_password: str
    ubki_upload_url: str
    ubki_auth_url: str
    db_path: Path
    api_token: str
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    retry_cap: int = 5
    min_file_age_sec: int = 300
    file_glob: str = "*.txt"
    max_line_bytes: int = 2 * 1024 * 1024
    network_abort_threshold: int = 3
    http_timeout_sec: float = 60.0
    # parallel send: worker pool size and a hard requests/second ceiling.
    # UBKI accepts up to 30 packets/sec; we send at 25 with margin. The pool
    # bounds simultaneous in-flight requests; the rate cap paces starts so the
    # bureau's per-second limit is never exceeded regardless of pool size.
    ubki_concurrency: int = 8
    ubki_max_rps: float = 25.0
    # enricher: raw producer files + cabinet MySQL (required only by app.enrich)
    raw_folder: Path | None = None
    mysql_host: str | None = None
    mysql_port: int = 3306
    mysql_user: str | None = None
    mysql_password: str | None = None
    mysql_db: str | None = None
    deal_vidobes: str = "90"  # dir.15 "no collateral" — injected when a deal lacks it

    @property
    def archive_folder(self) -> Path:
        return self.data_folder / "archive"

    @property
    def lock_path(self) -> Path:
        return self.db_path.parent / "run.lock"

    @property
    def enrich_lock_path(self) -> Path:
        return self.db_path.parent / "enrich.lock"

    @property
    def quarantine_folder(self) -> Path:
        assert self.raw_folder is not None
        return self.raw_folder / "quarantine"

    @property
    def processed_folder(self) -> Path:
        assert self.raw_folder is not None
        return self.raw_folder / "processed"


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing required env variable: {name}")
    return value


def load_config() -> Config:
    return Config(
        data_folder=Path(_require("UBKI_DATA_FOLDER_PATH")),
        ubki_login=_require("UBKI_LOGIN"),
        ubki_password=_require("UBKI_PASSWORD"),
        ubki_upload_url=os.environ.get("UBKI_URL", DEFAULT_UPLOAD_URL),
        ubki_auth_url=os.environ.get("UBKI_AUTH_URL", DEFAULT_AUTH_URL),
        db_path=Path(os.environ.get("DB_PATH", "/data/ubki.sqlite3")),
        api_token=_require("API_TOKEN"),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN") or None,
        telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID") or None,
        retry_cap=int(os.environ.get("RETRY_CAP") or 5),
        min_file_age_sec=int(os.environ.get("MIN_FILE_AGE_SEC") or 300),
        file_glob=os.environ.get("FILE_GLOB", "").strip() or "*.txt",
        ubki_concurrency=int(os.environ.get("UBKI_CONCURRENCY") or 8),
        ubki_max_rps=float(os.environ.get("UBKI_MAX_RPS") or 25.0),
        raw_folder=Path(raw) if (raw := os.environ.get("RAW_FOLDER", "").strip()) else None,
        mysql_host=os.environ.get("MYSQL_HOST") or None,
        mysql_port=int(os.environ.get("MYSQL_PORT") or 3306),
        mysql_user=os.environ.get("MYSQL_USER") or None,
        mysql_password=os.environ.get("MYSQL_PASSWORD") or None,
        mysql_db=os.environ.get("MYSQL_DB") or None,
        deal_vidobes=os.environ.get("DEAL_VIDOBES", "").strip() or "90",
    )
