"""load_config env parsing.

Config is normally built directly in tests (see the `cfg` fixture); this module
is the deliberate exception because it exercises load_config's env parsing
itself, which can only be reached through the environment.
"""

from __future__ import annotations

from app.config import load_config


def _base_env(monkeypatch, tmp_path) -> None:
    for key in ("UBKI_URL", "UBKI_AUTH_URL", "DB_PATH", "RETRY_CAP",
                "MIN_FILE_AGE_SEC", "MYSQL_PORT", "FILE_GLOB"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("UBKI_DATA_FOLDER_PATH", str(tmp_path / "inbox"))
    monkeypatch.setenv("UBKI_LOGIN", "login")
    monkeypatch.setenv("UBKI_PASSWORD", "password")
    monkeypatch.setenv("API_TOKEN", "token")


def test_empty_numeric_env_falls_back_to_default(monkeypatch, tmp_path):
    # present-but-empty numeric vars must NOT crash: the "5"/"300"/"3306"
    # defaults only apply when the key is absent, so int("") would raise.
    _base_env(monkeypatch, tmp_path)
    monkeypatch.setenv("RETRY_CAP", "")
    monkeypatch.setenv("MIN_FILE_AGE_SEC", "")
    monkeypatch.setenv("MYSQL_PORT", "")

    config = load_config()
    assert config.retry_cap == 5
    assert config.min_file_age_sec == 300
    assert config.mysql_port == 3306


def test_numeric_env_is_parsed_when_set(monkeypatch, tmp_path):
    _base_env(monkeypatch, tmp_path)
    monkeypatch.setenv("RETRY_CAP", "9")
    monkeypatch.setenv("MIN_FILE_AGE_SEC", "42")
    monkeypatch.setenv("MYSQL_PORT", "3307")

    config = load_config()
    assert (config.retry_cap, config.min_file_age_sec, config.mysql_port) == (9, 42, 3307)
