"""UbkiClient against httpx.MockTransport: envelope, state mapping, re-auth."""

import json

import httpx
import pytest

from app.db import FAILED, REJECTED, SENT
from app.ubki_client import UbkiAuthError, UbkiClient, build_envelope

RAW_LINE = '{"inn":"3418011570","reqlng":1,"deals":[{"dlref":"395397"}]}'
AUTH_OK = {"doc": {"auth": {"sessid": "SESS123", "datecr": "x", "dateed": "y"}}}


def make_client(cfg, handler) -> UbkiClient:
    return UbkiClient(cfg, transport=httpx.MockTransport(handler))


def upload_response(payload: dict) -> httpx.Response:
    return httpx.Response(200, json=payload)


def test_envelope_wraps_line_as_is():
    body = json.loads(build_envelope(RAW_LINE + "\n", "abc123"))
    assert body["reqtype"] == "u"
    assert body["reqreason"] == "0"
    assert body["reqidout"] == "abc123"
    assert body["data"]["fo_cki"] == json.loads(RAW_LINE)


def test_auth_extracts_sessid_and_sends_header(cfg):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            body = json.loads(request.content)
            assert body == {"doc": {"auth": {"login": "login", "pass": "password"}}}
            return httpx.Response(200, json=AUTH_OK)
        seen["sessid"] = request.headers.get("SessId")
        seen["content_type"] = request.headers.get("Content-Type")
        return upload_response({"state": "ok"})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == SENT
    assert seen["sessid"] == "SESS123"
    assert seen["content_type"] == "application/json"


def test_auth_failure_raises(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"doc": {"errcode": "1", "errtext": "bad creds"}})

    with make_client(cfg, handler) as client:
        with pytest.raises(UbkiAuthError, match="bad creds"):
            client.auth()


@pytest.mark.parametrize(
    "payload,expected_status,expected_state",
    [
        ({"state": "ok", "ok": 1, "nt": 0, "er": 0}, SENT, "ok"),
        ({"state": "nt", "ok": 0, "nt": 1, "er": 0}, SENT, "nt"),
        ({"state": "er", "errtext": "bad inn"}, REJECTED, "er"),
        ({"state": "sy"}, FAILED, "sy"),
    ],
)
def test_state_mapping(cfg, payload, expected_status, expected_state):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return upload_response(payload)

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == expected_status
    assert result.state == expected_state


def test_partial_component_rejection_maps_to_rejected(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return upload_response({"state": "ok", "ok": 2, "nt": 0, "er": 1})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == REJECTED
    assert "component" in result.error


def test_nt_sets_warning_flag(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return upload_response({"state": "nt", "ok": 0, "nt": 1, "er": 0})

    with make_client(cfg, handler) as client:
        assert client.upload_record(RAW_LINE, "rid1").has_warnings is True


def test_network_error_is_failed_and_flagged(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        raise httpx.ConnectError("boom")

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == FAILED
    assert result.is_network_error is True


def test_5xx_is_retryable_failed(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return httpx.Response(502, text="bad gateway")

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == FAILED
    assert result.is_network_error is True
    assert result.http_status == 502


def test_expired_session_reauths_once(cfg):
    calls = {"auth": 0, "upload": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            calls["auth"] += 1
            return httpx.Response(200, json=AUTH_OK)
        calls["upload"] += 1
        if calls["upload"] == 1:
            return upload_response({"errcode": 2014, "errtext": "no active session"})
        return upload_response({"state": "ok"})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == SENT
    assert calls == {"auth": 2, "upload": 2}


class MemoryStore:
    def __init__(self, sessid=None):
        self.sessid = sessid
        self.saved = []

    def load(self):
        return self.sessid

    def save(self, sessid):
        self.saved.append(sessid)


def test_cached_session_skips_auth(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        assert not request.url.path.endswith("/auth"), "auth must not be called"
        assert request.headers["SessId"] == "CACHED"
        return upload_response({"state": "ok"})

    client = UbkiClient(cfg, session_store=MemoryStore("CACHED"),
                        transport=httpx.MockTransport(handler))
    with client:
        assert client.upload_record(RAW_LINE, "rid1").status == SENT


def test_fresh_session_saved_to_store(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return upload_response({"state": "ok"})

    store = MemoryStore()
    with UbkiClient(cfg, session_store=store, transport=httpx.MockTransport(handler)) as client:
        client.upload_record(RAW_LINE, "rid1")
    assert store.saved == ["SESS123"]
