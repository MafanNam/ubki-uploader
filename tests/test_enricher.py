"""Enricher: fo_cki assembly rules, quarantine reasons, file flow."""

import json
from datetime import date, datetime

from app import db
from app.enricher import EnrichSummary, build_alert, enrich_line, normalize_phone, run_enrich

from .conftest import write_jsonl

INN = "3418011570"


def make_row(app_id=395397, user_id=77, **over):
    row = {
        "id": app_id, "user_id": user_id,
        "applied_at": datetime(2026, 1, 15, 10, 30),
        "snap_passport_number": "СЕ311111",
        "snap_passport_date": date(2014, 10, 1),
        "snap_passport_issued_by": "Луцьким МВ УДМС",
        "snap_phone": "380981220000",
        "addr_postcode": "43000", "addr_city": "Луцьк м.", "addr_street": "Науки просп.",
        "addr_house": "1", "addr_building": "", "addr_flat": "83",
        "user_inn": INN, "user_phone": "0509998877",
        "user_passport_number": None, "user_passport_date": None,
        "user_passport_issued_by": None,
    }
    row.update(over)
    return row


def make_line(**over):
    line = {
        "inn": INN, "reqlng": 1, "lname": "Іванов", "fname": "Іван",
        "mname": "Іванович", "bdate": "1993-07-31",
        "deals": [{
            "dlref": "395397", "lng": 1, "INN": INN, "dlcelcred": 7, "dlporpog": 7,
            "dlcurr": 980, "dlamt": 800, "dlds": "2018-03-01", "dlrolesub": 1,
            "deallife": [{"dlmonth": 7, "dlyear": 2026, "dlflstat": 1}],
        }],
    }
    line.update(over)
    return line


def fake_fetch(rows):
    def fetch(config, dlrefs):
        return {ref: rows[ref] for ref in dlrefs if ref in rows}
    return fetch


# --- enrich_line ------------------------------------------------------------

def test_enrich_line_builds_full_subject(cfg):
    subject, reason = enrich_line(make_line(), {"395397": make_row()}, cfg)

    assert reason is None
    assert subject["inn"] == INN
    assert subject["person_id"] == "77"
    assert subject["is_gone"] == "0"
    assert subject["reqlng"] == "1"

    ident = subject["idents"][0]
    assert ident["cgrag"] == "804"
    assert ident["vdate"] == "2026-01-15"
    assert (ident["lname"], ident["fname"], ident["mname"]) == ("Іванов", "Іван", "Іванович")

    doc = subject["docs"][0]
    assert (doc["dtype"], doc["dser"], doc["dnom"]) == ("1", "СЕ", "311111")
    assert doc["dwdt"] == "2014-10-01"

    addr = subject["addrs"][0]
    assert (addr["adtype"], addr["adcountry"]) == ("2", "804")
    assert addr["adcity"] == "Луцьк м."
    assert "adcorp" not in addr  # empty source fields are omitted
    assert "addrdirt" not in addr  # deprecated field never sent

    assert subject["contacts"] == [{"vdate": "2026-01-15", "ctype": "3", "cval": "+380981220000"}]

    deal = subject["deals"][0]
    assert deal["dlvidobes"] == "90"  # injected mandatory field
    assert deal["INN"] == INN  # unknown keys pass through untouched
    assert deal["deallife"] == [{"dlmonth": 7, "dlyear": 2026, "dlflstat": 1}]


def test_id_card_is_dtype_17_with_derived_dterm(cfg):
    row = make_row(snap_passport_number="123456789")
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert reason is None
    doc = subject["docs"][0]
    assert (doc["dtype"], doc["dser"], doc["dnom"]) == ("17", "", "123456789")
    # bureau requires dterm for ID cards (live 3003); derived as issue + 10y
    assert doc["dwdt"] == "2014-10-01"
    assert doc["dterm"] == "2024-10-01"


def test_id_card_without_issue_date_quarantines(cfg):
    row = make_row(snap_passport_number="123456789", snap_passport_date=None,
                   user_passport_number=None)
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert subject is None
    assert "dterm" in reason


def test_passport_falls_back_to_users_row(cfg):
    row = make_row(snap_passport_number="", user_passport_number="АБ 123456",
                   user_passport_date=date(2010, 5, 5),
                   user_passport_issued_by="Луцьким РВ УМВС")
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert reason is None
    doc = subject["docs"][0]
    assert (doc["dtype"], doc["dser"], doc["dnom"]) == ("1", "АБ", "123456")
    assert doc["dwdt"] == "2010-05-05"


def test_unsupported_passport_quarantines(cfg):
    row = make_row(snap_passport_number="123456", user_passport_number=None)
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert subject is None
    assert "passport" in reason


def test_empty_document_issuer_quarantines(cfg):
    """Live finding: the bureau drops docs without dwho (IGNORED 3003) and
    then rejects the package (2077) — block such lines locally instead."""
    row = make_row(snap_passport_issued_by="", user_passport_number=None)
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert subject is None
    assert "dwho" in reason


def test_issuer_fallback_to_users_doc(cfg):
    # snapshot has a valid number but no issuer; users has a complete doc
    row = make_row(snap_passport_issued_by="",
                   user_passport_number="123456789",
                   user_passport_issued_by="8888",
                   user_passport_date=date(2020, 2, 29))
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert reason is None
    doc = subject["docs"][0]
    assert (doc["dtype"], doc["dnom"], doc["dwho"]) == ("17", "123456789", "8888")
    assert doc["dterm"] == "2030-02-28"  # leap-day issue date handled


def test_inn_mismatch_quarantines(cfg):
    row = make_row(user_inn="9999999999")
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert subject is None
    assert "inn mismatch" in reason


def test_unknown_dlref_quarantines(cfg):
    subject, reason = enrich_line(make_line(), {}, cfg)
    assert subject is None
    assert "395397" in reason and "not found" in reason


def test_deals_of_different_clients_quarantine(cfg):
    line = make_line(deals=[{"dlref": "1"}, {"dlref": "2"}])
    rows = {"1": make_row(app_id=1, user_id=10), "2": make_row(app_id=2, user_id=20)}
    subject, reason = enrich_line(line, rows, cfg)
    assert subject is None
    assert "different clients" in reason


def test_newest_application_wins(cfg):
    line = make_line(deals=[{"dlref": "1"}, {"dlref": "2"}])
    rows = {
        "1": make_row(app_id=1, applied_at=datetime(2024, 3, 1), snap_phone="0671112233"),
        "2": make_row(app_id=2, applied_at=datetime(2026, 2, 2), snap_phone="0679998877"),
    }
    subject, reason = enrich_line(line, rows, cfg)
    assert reason is None
    assert subject["idents"][0]["vdate"] == "2026-02-02"
    assert subject["contacts"][0]["cval"] == "+380679998877"


def test_phone_fallback_and_quarantine(cfg):
    row = make_row(snap_phone="not a phone")
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert reason is None
    assert subject["contacts"][0]["cval"] == "+380509998877"  # from users

    row = make_row(snap_phone="", user_phone="12345")
    subject, reason = enrich_line(make_line(), {"395397": row}, cfg)
    assert subject is None
    assert "phone" in reason


def test_normalize_phone_variants():
    assert normalize_phone("380981220000") == "+380981220000"
    assert normalize_phone("+380981220000") == "+380981220000"
    assert normalize_phone("098 122 00 00") == "+380981220000"
    assert normalize_phone("0981220000") == "+380981220000"
    assert normalize_phone("1234") is None
    assert normalize_phone(None) is None


# --- run_enrich (file flow) ---------------------------------------------------

def test_run_enrich_writes_inbox_quarantine_and_processed(cfg):
    lines = [
        json.dumps(make_line(), ensure_ascii=False),
        "{broken json",
        json.dumps(make_line(inn="0000000000"), ensure_ascii=False),  # inn mismatch
    ]
    write_jsonl(cfg.raw_folder, "a.jsonl", lines)
    summary = run_enrich(cfg, fetch=fake_fetch({"395397": make_row()}))

    assert (summary.files_seen, summary.files_processed) == (1, 1)
    assert (summary.lines_total, summary.lines_enriched, summary.lines_quarantined) == (3, 1, 2)

    enriched = (cfg.data_folder / "a.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(enriched) == 1
    assert json.loads(enriched[0])["person_id"] == "77"

    quarantine = [json.loads(l) for l in
                  (cfg.quarantine_folder / "a.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [q["line_no"] for q in quarantine] == [2, 3]
    assert "broken JSON" in quarantine[0]["reason"]
    assert "inn mismatch" in quarantine[1]["reason"]

    assert not (cfg.raw_folder / "a.jsonl").exists()
    assert (cfg.processed_folder / "a.jsonl").exists()

    alert = build_alert(summary)
    assert "карантині: 2" in alert


def test_run_enrich_is_idempotent_by_identity(cfg):
    write_jsonl(cfg.raw_folder, "a.jsonl", [json.dumps(make_line(), ensure_ascii=False)])
    run_enrich(cfg, fetch=fake_fetch({"395397": make_row()}))

    # same content dropped again: identity (filename + sha256) skips it
    write_jsonl(cfg.raw_folder, "a.jsonl", [json.dumps(make_line(), ensure_ascii=False)])
    summary = run_enrich(cfg, fetch=fake_fetch({"395397": make_row()}))
    assert summary.files_processed == 0
    assert len((cfg.data_folder / "a.jsonl").read_text(encoding="utf-8").splitlines()) == 1


def test_dry_run_touches_nothing(cfg):
    write_jsonl(cfg.raw_folder, "a.jsonl", [json.dumps(make_line(), ensure_ascii=False)])

    def exploding_fetch(config, dlrefs):
        raise AssertionError("dry-run must not query MySQL")

    summary = run_enrich(cfg, fetch=exploding_fetch, dry_run=True)
    assert summary.files_processed == 1
    assert summary.lines_total == 1
    assert (cfg.raw_folder / "a.jsonl").exists()
    assert not (cfg.data_folder / "a.jsonl").exists()
    conn = db.connect(cfg.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) AS n FROM enriched_files").fetchone()["n"] == 0
    finally:
        conn.close()


def test_quarantine_file_roundtrips_back_through_raw(cfg):
    # first pass: no DB rows -> the line lands in quarantine
    write_jsonl(cfg.raw_folder, "a.jsonl", [json.dumps(make_line(), ensure_ascii=False)])
    run_enrich(cfg, fetch=fake_fetch({}))
    qfile = cfg.quarantine_folder / "a.jsonl"
    assert qfile.exists()

    # data fixed in the cabinet: drop the quarantine file back into raw as-is
    write_jsonl(cfg.raw_folder, "a.jsonl", qfile.read_text(encoding="utf-8").splitlines())
    summary = run_enrich(cfg, fetch=fake_fetch({"395397": make_row()}))
    assert summary.lines_enriched == 1

    enriched = (cfg.data_folder / "a.jsonl").read_text(encoding="utf-8").splitlines()
    assert json.loads(enriched[0])["inn"] == INN


def test_alert_none_when_clean():
    assert build_alert(EnrichSummary(lines_enriched=5)) is None
