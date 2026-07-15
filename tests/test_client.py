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


def test_main_errcode_2014_in_sentdatainfo_reauths_once(cfg):
    calls = {"auth": 0, "upload": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            calls["auth"] += 1
            return httpx.Response(200, json=AUTH_OK)
        calls["upload"] += 1
        if calls["upload"] == 1:
            return upload_response({"sentdatainfo": {
                "state": "er", "main_errcode": 2014,
                "items": [{"errtype": "CRITICAL", "errcode": 2014, "msg": "no session"}],
            }})
        return upload_response({"sentdatainfo": {"state": "ok", "ok": 1, "er": 0, "ig": 0}})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == SENT
    assert calls == {"auth": 2, "upload": 2}


def test_item_errcode_2001_in_rejection_does_not_reauth(cfg):
    """Per-component errcodes inside items[] must never trigger a re-auth."""
    calls = {"auth": 0, "upload": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            calls["auth"] += 1
            return httpx.Response(200, json=AUTH_OK)
        calls["upload"] += 1
        return upload_response({"sentdatainfo": {
            "state": "er", "main_errcode": 2001, "ok": 0, "er": 1,
            "items": [{"errtype": "CRITICAL", "errcode": 2001, "msg": "unexpected error"}],
        }})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == REJECTED
    assert calls == {"auth": 1, "upload": 1}


def test_ig_counter_maps_to_sent_with_warning(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return upload_response({"sentdatainfo": {
            "state": "ok", "main_errcode": 0, "ok": 2, "nt": 0, "ig": 1, "er": 0, "sy": 0,
            "items": [{"errtype": "IGNORED", "errcode": 2045, "msg": "date in the future"}],
        }})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == SENT
    assert result.has_warnings is True


def test_rejection_error_includes_items_detail(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return upload_response({"sentdatainfo": {
            "state": "er", "main_errcode": 2031, "ok": 0, "er": 1,
            "items": [{"errtype": "CRITICAL", "errcode": 2031, "msg": "невірний ІПН"}],
        }})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == REJECTED
    assert "rejected by UBKI (state=er)" in result.error
    assert "main_errcode=2031" in result.error
    assert "невірний ІПН" in result.error


def test_http_400_with_er_body_is_rejected_not_failed(cfg):
    """UBKI pairs validation rejections with HTTP 400 (observed live): the
    body's state=er must win over the HTTP code and map to `rejected`."""
    calls = {"auth": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            calls["auth"] += 1
            return httpx.Response(200, json=AUTH_OK)
        return httpx.Response(400, json={
            "reqinfo": {"reqid": "IN#X", "reqidout": "rid1"},
            "http_status": 400,
            "sentdatainfo": {
                "inn": "0000000000", "ok": 0, "nt": 1, "ig": 0, "er": 4, "sy": 0,
                "items": [
                    {"tag": "IDENT", "compid": 1, "errtype": "CRITICAL", "errcode": 2078,
                     "msg": "Відсутній валідний блок ідентифікації"},
                    {"tag": "CRDEAL", "compid": 2, "errtype": "NOTICE", "errcode": 5001,
                     "msg": "OK NEW"},
                ],
                "state": "er", "main_errcode": 2077,
            },
        })

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == REJECTED
    assert result.is_network_error is False
    assert result.http_status == 400
    assert "main_errcode=2077" in result.error
    assert "2078" in result.error
    assert calls["auth"] == 1  # no re-auth on a validation rejection


def test_http_400_without_state_stays_failed(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return httpx.Response(400, json={"message": "gateway said no"})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == FAILED
    assert result.error == "HTTP 400"
    assert result.is_network_error is False


def test_sy_state_counts_as_network_error(cfg):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            return httpx.Response(200, json=AUTH_OK)
        return upload_response({"sentdatainfo": {"state": "sy", "main_errcode": 1000}})

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == FAILED
    assert result.is_network_error is True


def test_session_rejected_twice_is_network_error(cfg):
    """Persistent 401 on upload must count toward the pass abort (no per-record
    auth storm: UBKI forbids frequent re-auth)."""
    calls = {"auth": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth"):
            calls["auth"] += 1
            return httpx.Response(200, json=AUTH_OK)
        return httpx.Response(401, text="unauthorized")

    with make_client(cfg, handler) as client:
        result = client.upload_record(RAW_LINE, "rid1")

    assert result.status == FAILED
    assert result.error == "session rejected twice"
    assert result.is_network_error is True
    assert calls["auth"] == 2  # initial + the single re-auth


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
