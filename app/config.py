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
    max_line_bytes: int = 2 * 1024 * 1024
    network_abort_threshold: int = 3
    http_timeout_sec: float = 60.0

    @property
    def archive_folder(self) -> Path:
        return self.data_folder / "archive"

    @property
    def lock_path(self) -> Path:
        return self.db_path.parent / "run.lock"


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
        retry_cap=int(os.environ.get("RETRY_CAP", "5")),
        min_file_age_sec=int(os.environ.get("MIN_FILE_AGE_SEC", "300")),
    )
