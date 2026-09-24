"""delete_period: envelope shape and the guards that keep experiments off prod."""

import json

import delete_period as dp

TEST_URL = "https://test.ubki.ua/upload/data"
PROD_URL = "https://secure.ubki.ua/upload/data"


def test_envelope_embeds_the_line_verbatim_with_delreason():
    line = '{"inn":"1","deals":[{"dlref":"7"}]}\n'
    env = dp.build_envelope(line, "abc", "d", "27").decode("utf-8")
    assert env == ('{"reqtype":"d","reqidout":"abc","reqreason":"0","delreason":"27",'
                   '"data":{"fo_cki":{"inn":"1","deals":[{"dlref":"7"}]}}}')
    assert json.loads(env)["data"]["fo_cki"]["deals"][0]["dlref"] == "7"


def test_update_envelope_has_no_delreason():
    env = json.loads(dp.build_envelope('{"inn":"1"}', "x", "u", None))
    assert env["reqtype"] == "u" and "delreason" not in env


def test_delete_requires_a_partner_delreason():
    assert dp.check_args("d", None, TEST_URL, False)
    assert dp.check_args("d", "11", TEST_URL, False)   # bureau-only code
    assert dp.check_args("d", "27", TEST_URL, False) is None
    assert dp.check_args("u", "27", TEST_URL, False)


def test_prod_needs_an_explicit_flag_and_test_refuses_it():
    assert "--prod" in dp.check_args("d", "27", PROD_URL, False)
    assert dp.check_args("d", "27", PROD_URL, True) is None
    assert dp.check_args("u", None, TEST_URL, True)


def test_summarize_response_extracts_items():
    text = json.dumps({"reqinfo": {"reqid": "IN#1"}, "sentdatainfo": {
        "state": "er", "main_errcode": 3004, "ok": 3, "er": 1,
        "items": [{"errtype": "CRITICAL", "errcode": 3004, "tag": "CRDEAL", "msg": "m"}]}})
    s = dp.summarize_response(text)
    assert s["state"] == "er" and s["reqid"] == "IN#1" and s["counters"]["er"] == 1
    assert s["items"] == [{"errtype": "CRITICAL", "errcode": 3004, "tag": "CRDEAL", "msg": "m"}]
    assert dp.summarize_response("<html>")["raw"] == "<html>"


def test_read_sessid_is_read_only(tmp_path):
    import sqlite3

    path = tmp_path / "u.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO meta VALUES ('ubki_sessid', 'S')")
    conn.commit()
    conn.close()
    before = path.read_bytes()
    assert dp.read_sessid(str(path)) == "S"
    assert path.read_bytes() == before


def test_package_dlrefs():
    assert dp.package_dlrefs('{"deals":[{"dlref":"7"},{"dlref":8}]}') == ["7", "8"]
    assert dp.package_dlrefs("not json") == []
