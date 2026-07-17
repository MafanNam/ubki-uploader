"""UBKI HTTP client: auth (SessId) + per-record upload.

Endpoints (see UBKI wiki, spaces/Spec pages 112984161 / 114983026 and
spaces/Specification page 117342209):

- auth:   POST {UBKI_AUTH_URL}  body {"doc": {"auth": {"login", "pass"}}}
          response contains `sessid`, valid until 23:59:59 Kyiv time same day
- upload: POST {UBKI_URL}       body {"reqtype","reqidout","reqreason","data":{"fo_cki":...}}
          header `SessId`; response payload lives under `sentdatainfo`:
          {"sentdatainfo": {"state": ok|nt|er|sy, "main_errcode": N,
                            "ok"/"nt"/"ig"/"er"/"sy": counters,
                            "items": [{"errtype", "errcode", "msg", ...}]}}
          (confirmed live on test.ubki.ua 2026-07-14). Counter semantics:
          nt = accepted with notices, ig = component dropped but package
          accepted, er = component caused package rejection.

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
# 2014 = no active session (doc) -> re-auth once and retry. 2001 (session
# expired, observed live) arrives with HTTP 401 and is handled by the 401/403
# branch; on a 200 body 2001 means "unexpected error" per the wiki, so it must
# NOT trigger a re-auth.
SESSION_EXPIRED_ERRCODES = {"2014"}
# stored into records.ubki_response; generous enough that a full sentdatainfo
# with items[] survives intact (a truncated body no longer parses as JSON)
RESPONSE_TEXT_LIMIT = 8192


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


def _sentdatainfo(data) -> dict:
    """The documented upload response keeps its payload under `sentdatainfo`;
    fall back to the body itself for flat/variant layouts."""
    if isinstance(data, dict):
        info = data.get("sentdatainfo")
        if isinstance(info, dict):
            return info
        return data
    return {}


def _counter(info: dict, key: str) -> int:
    try:
        return int(info.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _rejection_detail(info: dict) -> str:
    """main_errcode + first item messages — goes into records.last_error so the
    rejection reason is readable without digging through the raw response."""
    parts = []
    main = info.get("main_errcode")
    if main not in (None, "", 0, "0"):
        parts.append(f"main_errcode={main}")
    items = info.get("items")
    if isinstance(items, list):
        for item in items[:3]:
            if not isinstance(item, dict):
                continue
            code = " ".join(
                str(item[key]) for key in ("errtype", "errcode") if item.get(key) is not None
            )
            msg = item.get("msg")
            if code and msg:
                parts.append(f"[{code}] {msg}")
            elif code or msg:
                parts.append(str(msg or code))
    return "; ".join(parts)[:500]


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
        # A session rejected even after a fresh auth means nothing will go
        # through this pass (and re-authing per record is exactly what UBKI
        # forbids) — flag it network-like so the 3-strike abort kicks in.
        return self._post_record(raw_line, reqidout, sessid) or UploadResult(
            status=FAILED, error="session rejected twice", is_network_error=True
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

        try:
            data = resp.json()
        except ValueError:
            data = None

        if data is not None:
            info = _sentdatainfo(data)
            if resp.status_code == 200:
                # Session errors surface as sentdatainfo.main_errcode (or a
                # top-level errcode in flat variants) on a 200 body; on any
                # other status the HTTP code decides. Per-component errcodes
                # inside items[] must never trigger a re-auth.
                errcode = info.get("main_errcode")
                if errcode is None and isinstance(data, dict):
                    errcode = data.get("errcode")
                if errcode is not None and str(errcode) in SESSION_EXPIRED_ERRCODES:
                    self._sessid = None
                    return None
            # state and the counters are read from the same (shallow) `info`
            # so a variant layout can't yield a state without its counters.
            state = info.get("state")
            state = str(state).lower() if state is not None else None
            # For client errors a body carrying `state` is authoritative over
            # the HTTP code: UBKI pairs validation rejections (state=er) with
            # HTTP 400 (observed live 2026-07-15). A 5xx always stays a
            # transport failure — a gateway error page containing some
            # "state" field must not defeat the network-abort logic.
            if resp.status_code < 500 and (resp.status_code == 200 or state is not None):
                return self._map_state(info, state, resp.status_code, text)

        if resp.status_code >= 500:
            return UploadResult(
                status=FAILED, http_status=resp.status_code,
                response_text=text[:RESPONSE_TEXT_LIMIT], error=f"HTTP {resp.status_code}",
                is_network_error=True,
            )
        if resp.status_code != 200:
            return UploadResult(
                status=FAILED, http_status=resp.status_code,
                response_text=text[:RESPONSE_TEXT_LIMIT], error=f"HTTP {resp.status_code}",
            )
        return UploadResult(
            status=FAILED, http_status=resp.status_code,
            response_text=text[:RESPONSE_TEXT_LIMIT], error="non-JSON response",
        )

    @staticmethod
    def _map_state(info: dict, state: str | None, http_status: int, text: str) -> UploadResult:
        response_text = text[:RESPONSE_TEXT_LIMIT]

        if state in ("ok", "nt"):
            er_count = _counter(info, "er")
            if er_count > 0:
                detail = _rejection_detail(info)
                error = f"{er_count} component(s) rejected within accepted request"
                return UploadResult(
                    status=REJECTED, state=state, http_status=http_status,
                    response_text=response_text,
                    error=f"{error}: {detail}" if detail else error,
                )
            # nt = component accepted with notices (seen live: state=ok with
            # nt>0 when the test base substituted the INN), ig = component
            # dropped but package accepted: both must reach the operator as
            # warnings even when the overall state is "ok".
            has_warnings = (
                state == "nt" or _counter(info, "nt") > 0 or _counter(info, "ig") > 0
            )
            return UploadResult(
                status=SENT, state=state, http_status=http_status,
                response_text=response_text, has_warnings=has_warnings,
            )
        if state == "er":
            detail = _rejection_detail(info)
            error = "rejected by UBKI (state=er)"
            return UploadResult(
                status=REJECTED, state=state, http_status=http_status,
                response_text=response_text,
                error=f"{error}: {detail}" if detail else error,
            )
        if state == "sy":
            # SYSTEM errors: the wiki asks clients to back off while they
            # last, so count them toward the 3-strike pass abort.
            return UploadResult(
                status=FAILED, state=state, http_status=http_status,
                response_text=response_text, error="UBKI system error (state=sy)",
                is_network_error=True,
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
