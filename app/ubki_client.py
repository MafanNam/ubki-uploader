"""UBKI HTTP client: auth (SessId) + per-record upload.

Endpoints (see UBKI wiki, spaces/Spec pages 112984161 / 114983026 and
spaces/Specification page 117342209):

- auth:   POST {UBKI_AUTH_URL}  body {"doc": {"auth": {"login", "pass"}}}
          response contains `sessid`, valid until 23:59:59 Kyiv time same day
- upload: POST {UBKI_URL}       body {"reqtype","reqidout","reqreason","data":{"fo_cki":...}}
          header `SessId`; response `state` in ok|nt|er|sy + ok/nt/er counters

The raw JSONL line is embedded into the envelope byte-for-byte (the service
must not parse or validate file contents).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from zoneinfo import ZoneInfo

import httpx

from .config import KYIV_TZ, Config
from .db import FAILED, REJECTED, SENT

log = logging.getLogger("ubki.client")

REQTYPE_UPDATE = "u"  # 'd' (deletion) is out of scope for v1
REQREASON_TRANSMISSION = "0"
# 2014 = no active session (doc); 2001 = UNAUTHORIZED validation.session.is.expired
# (observed live on test.ubki.ua, arrives with HTTP 401)
SESSION_EXPIRED_ERRCODES = {"2014", "2001"}


class UbkiAuthError(Exception):
    pass


class SessionStore(Protocol):
    """Persists sessid across runs (UBKI asks to authenticate once per day)."""

    def load(self) -> str | None: ...
    def save(self, sessid: str) -> None: ...


@dataclass
class UploadResult:
    status: str  # SENT | FAILED | REJECTED
    state: str | None = None
    http_status: int | None = None
    response_text: str | None = None
    error: str | None = None
    is_network_error: bool = False
    has_warnings: bool = False


def kyiv_today() -> str:
    return datetime.now(ZoneInfo(KYIV_TZ)).date().isoformat()


def _find_key(data, key: str):
    """Depth-first search for a key in nested dicts/lists (response layout
    differs between doc examples, so we locate fields defensively)."""
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for value in data.values():
            found = _find_key(value, key)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _find_key(item, key)
            if found is not None:
                return found
    return None


def build_envelope(raw_line: str, reqidout: str) -> bytes:
    """Wrap one JSONL line as-is; the line itself is never re-serialized."""
    return (
        '{"reqtype":"%s","reqidout":"%s","reqreason":"%s","data":{"fo_cki":%s}}'
        % (REQTYPE_UPDATE, reqidout, REQREASON_TRANSMISSION, raw_line.strip())
    ).encode("utf-8")


class UbkiClient:
    def __init__(self, config: Config, session_store: SessionStore | None = None,
                 transport: httpx.BaseTransport | None = None):
        self._auth_url = config.ubki_auth_url
        self._upload_url = config.ubki_upload_url
        self._login = config.ubki_login
        self._password = config.ubki_password
        self._store = session_store
        self._sessid: str | None = None
        self._client = httpx.Client(timeout=config.http_timeout_sec, transport=transport)

    # --- session -----------------------------------------------------------

    def auth(self) -> str:
        body = {"doc": {"auth": {"login": self._login, "pass": self._password}}}
        try:
            resp = self._client.post(
                self._auth_url,
                json=body,
                headers={"Accept": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise UbkiAuthError(f"auth network error: {exc}") from exc
        if resp.status_code != 200:
            raise UbkiAuthError(f"auth HTTP {resp.status_code}: {resp.text[:500]}")
        try:
            data = resp.json()
        except ValueError as exc:
            raise UbkiAuthError(f"auth returned non-JSON: {resp.text[:500]}") from exc
        sessid = _find_key(data, "sessid")
        if not sessid:
            errtext = _find_key(data, "errtext") or resp.text[:500]
            raise UbkiAuthError(f"auth rejected: {errtext}")
        self._sessid = str(sessid)
        if self._store:
            self._store.save(self._sessid)
        log.info("ubki auth ok", extra={"event": "auth_ok"})
        return self._sessid

    def _ensure_session(self) -> str:
        if self._sessid:
            return self._sessid
        if self._store:
            cached = self._store.load()
            if cached:
                self._sessid = cached
                return cached
        return self.auth()

    # --- upload --------------------------------------------------------------

    def upload_record(self, raw_line: str, reqidout: str) -> UploadResult:
        try:
            sessid = self._ensure_session()
        except UbkiAuthError as exc:
            return UploadResult(status=FAILED, error=str(exc), is_network_error=True)

        result = self._post_record(raw_line, reqidout, sessid)
        if result is not None:
            return result
        # session was stale (2014 / 401): re-auth once and retry
        try:
            sessid = self.auth()
        except UbkiAuthError as exc:
            return UploadResult(status=FAILED, error=str(exc), is_network_error=True)
        return self._post_record(raw_line, reqidout, sessid) or UploadResult(
            status=FAILED, error="session rejected twice"
        )

    def _post_record(self, raw_line: str, reqidout: str, sessid: str) -> UploadResult | None:
        """Returns None when the session must be refreshed and the call retried."""
        try:
            resp = self._client.post(
                self._upload_url,
                content=build_envelope(raw_line, reqidout),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "SessId": sessid,
                },
            )
        except httpx.HTTPError as exc:
            return UploadResult(status=FAILED, error=f"network: {exc}", is_network_error=True)

        text = resp.text
        if resp.status_code in (401, 403):
            log.warning(
                "upload returned %s, refreshing session",
                resp.status_code,
                extra={"event": "session_rejected", "http_status": resp.status_code,
                       "body": text[:500]},
            )
            self._sessid = None
            return None
        if resp.status_code >= 500:
            return UploadResult(
                status=FAILED, http_status=resp.status_code,
                response_text=text[:2000], error=f"HTTP {resp.status_code}",
                is_network_error=True,
            )
        if resp.status_code != 200:
            return UploadResult(
                status=FAILED, http_status=resp.status_code,
                response_text=text[:2000], error=f"HTTP {resp.status_code}",
            )

        try:
            data = resp.json()
        except ValueError:
            return UploadResult(
                status=FAILED, http_status=resp.status_code,
                response_text=text[:2000], error="non-JSON response",
            )

        errcode = _find_key(data, "errcode")
        if errcode is not None and str(errcode) in SESSION_EXPIRED_ERRCODES:
            self._sessid = None
            return None

        return self._map_state(data, resp.status_code, text)

    @staticmethod
    def _map_state(data, http_status: int, text: str) -> UploadResult:
        state = _find_key(data, "state")
        state = str(state).lower() if state is not None else None
        response_text = text[:2000]

        if state in ("ok", "nt"):
            er_count = _find_key(data, "er")
            try:
                er_count = int(er_count) if er_count is not None else 0
            except (TypeError, ValueError):
                er_count = 0
            if er_count > 0:
                return UploadResult(
                    status=REJECTED, state=state, http_status=http_status,
                    response_text=response_text,
                    error=f"{er_count} component(s) rejected within accepted request",
                )
            return UploadResult(
                status=SENT, state=state, http_status=http_status,
                response_text=response_text, has_warnings=(state == "nt"),
            )
        if state == "er":
            return UploadResult(
                status=REJECTED, state=state, http_status=http_status,
                response_text=response_text, error="rejected by UBKI (state=er)",
            )
        if state == "sy":
            return UploadResult(
                status=FAILED, state=state, http_status=http_status,
                response_text=response_text, error="UBKI system error (state=sy)",
            )
        return UploadResult(
            status=FAILED, state=state, http_status=http_status,
            response_text=response_text, error=f"unknown state: {state!r}",
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "UbkiClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
