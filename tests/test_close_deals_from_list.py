"""close_deals_from_list: closing-slice shape, grouping against what UBKI
accepted, identity guards, canary pick, outputs."""

import json
import sqlite3
from datetime import date
from decimal import Decimal

import close_deals_from_list as cl
from app.enricher import _inn_birthday

from .test_enricher import INN, make_row

TODAY = date(2026, 9, 24)
BORN = _inn_birthday(INN)


def entry(dlref="395397", mark="", closed=date(2026, 8, 17), inn=INN):
    return cl.Entry(dlref=dlref, inn=inn, mark=mark, closed=closed)


def sent_deal(dlref="395397", year=2026, month=8, flstat=1):
    return {"dlref": dlref, "lng": 1, "INN": INN, "dlcelcred": 7, "dlporpog": 7,
            "dlcurr": 980, "dlamt": 800, "dlds": "2018-03-01", "dlrolesub": 1,
            "deallife": [{"dlref": dlref, "dlmonth": month, "dlyear": year,
                          "dldpf": "2018-03-04", "dlflstat": flstat, "dlamtlim": 800,
                          "dlamtcur": 800, "dlamtpaym": 0, "dldayexp": 3062,
                          "dlamtexp": 800, "dlflpay": 0, "dlflbrk": 1, "dlfluse": 0,
                          "dldateclc": f"{year}-{month:02d}-15"}],
            "dlvidobes": "90"}


def sent_subject(deal):
    return {"reqlng": "1", "inn": INN, "person_id": "77", "is_gone": "0",
            "lname": "Іваненко", "fname": "Іван", "mname": "Іванович", "bdate": BORN,
            "deals": [deal]}


def history(*periods, final=False, deal=None):
    h = cl.History(seen=True, final_accepted=final)
    h.accepted = set(periods)
    deal = deal or sent_deal()
    h.subject, h.deal = sent_subject(deal), deal
    return h


def header(dlref="395397", **over):
    h = {"id": int(dlref), "loan_amount": Decimal("1201.00"), "applied_at": "2016-09-09",
         "payment_date": "2016-11-08", "first_name": "Петро", "last_name": "Петренко",
         "other_name": "Петрович", "birth_date": BORN, "loan_closed": 1}
    h.update(over)
    return h


def run(entries, hist, rows=None, headers=None, cfg=None):
    rows = rows if rows is not None else {e.dlref: make_row(app_id=int(e.dlref)) for e in entries}
    headers = headers if headers is not None else {e.dlref: header(e.dlref) for e in entries}
    return cl.build_all(entries, hist, rows, headers, cfg, TODAY)


def only_slice(res):
    subject = json.loads(res["line"])
    (deal,) = subject["deals"]
    (s,) = deal["deallife"]
    return subject, deal, s


# --- slice -------------------------------------------------------------------

def test_closing_slice_is_zeroed_inside_the_dldff_month(cfg):
    (res,) = run([entry()], {"395397": history((2026, 8))}, cfg=cfg)
    _, _, s = only_slice(res)
    assert (s["dlyear"], s["dlmonth"]) == (2026, 8)
    assert s["dldff"] == s["dldateclc"] == "2026-08-17"
    assert s["dlflstat"] == cl.STATUS_CLOSED
    assert s["dlamtcur"] == s["dlamtexp"] == s["dldayexp"] == s["dlamtpaym"] == 0
    assert (s["dlflpay"], s["dlflbrk"], s["dlfluse"]) == (1, 0, 0)
    assert s["dldpf"] == "2018-03-04"   # kept from the accepted line


def test_bankruptcy_mark_selects_status_12(cfg):
    e = entry(mark="Списаний (банкрутство)")
    (res,) = run([e], {"395397": history((2026, 8))}, cfg=cfg)
    assert only_slice(res)[2]["dlflstat"] == cl.STATUS_BANKRUPTCY


def test_other_marks_close_as_2(cfg):
    for mark in ("вп", "Вп", "не передано", ""):
        assert entry(mark=mark).status == cl.STATUS_CLOSED


# --- grouping ------------------------------------------------------------------

def test_classify_groups():
    assert cl.classify(entry(), None) == "D"
    assert cl.classify(entry(), history((2026, 9), final=True)) == "A"
    rejected_only = history()
    assert cl.classify(entry(), rejected_only) == "E"
    assert cl.classify(entry(closed=date(2026, 8, 5)), history((2026, 8))) == "B"
    assert cl.classify(entry(closed=date(2026, 9, 5)), history((2026, 8))) == "B"
    assert cl.classify(entry(closed=date(2026, 8, 25)), history((2026, 8), (2026, 9))) == "C"


def test_final_and_locked_deals_get_no_line(cfg):
    a, c = entry("1"), entry("2", closed=date(2026, 8, 25))
    hist = {"1": history((2026, 9), final=True, deal=sent_deal("1")),
            "2": history((2026, 9), deal=sent_deal("2"))}
    res = {r["dlref"]: r for r in run([a, c], hist, cfg=cfg)}
    assert res["1"]["line"] is None and "2090" in res["1"]["reason"]
    assert res["2"]["line"] is None and "3004" in res["2"]["reason"]
    assert res["2"]["last_accepted"] == "2026-09"


def test_future_closing_date_is_refused(cfg):
    (res,) = run([entry(closed=date(2026, 10, 1))], {}, cfg=cfg)
    assert res["line"] is None and "future" in res["reason"]


# --- never-sent deals: header and names from the cabinet -------------------------

def test_never_sent_deal_is_built_from_the_cabinet(cfg):
    (res,) = run([entry(closed=date(2025, 3, 25))], {}, cfg=cfg)
    assert res["group"] == "D"
    subject, deal, s = only_slice(res)
    assert deal["dlamt"] == 1201 and deal["dlds"] == "2016-09-09"
    assert (deal["dlcelcred"], deal["dlporpog"], deal["dlcurr"], deal["dlrolesub"]) == (7, 7, 980, 1)
    assert deal["dlvidobes"] == "90"
    assert s["dldpf"] == "2016-11-08" and (s["dlyear"], s["dlmonth"]) == (2025, 3)
    assert (subject["lname"], subject["fname"], subject["mname"]) == ("Петренко", "Петро", "Петрович")
    assert subject["bdate"] == BORN
    assert subject["docs"] and subject["contacts"] and subject["addrs"]


def test_typographic_apostrophe_is_normalized(cfg):
    hdr = {"395397": header(other_name="В’ячеславівна")}
    (res,) = run([entry(closed=date(2025, 3, 25))], {}, headers=hdr, cfg=cfg)
    assert only_slice(res)[0]["mname"] == "В'ячеславівна"


def test_accepted_header_wins_over_the_cabinet(cfg):
    (res,) = run([entry()], {"395397": history((2026, 8))}, cfg=cfg)
    subject, deal, _ = only_slice(res)
    assert deal["dlamt"] == 800 and deal["dlds"] == "2018-03-01"
    assert subject["lname"] == "Іваненко"


# --- identity guards -------------------------------------------------------------

def test_list_inn_adopted_only_when_its_date_matches_the_cabinet(cfg):
    rows = {"395397": make_row(user_inn=None)}
    (res,) = run([entry(closed=date(2025, 3, 25))], {}, rows=rows, cfg=cfg)
    assert res["line"] and res["list_inn_adopted"]
    assert json.loads(res["line"])["inn"] == INN


def test_list_inn_refused_when_its_date_contradicts_the_cabinet(cfg):
    rows = {"395397": make_row(user_inn=None)}
    hdr = {"395397": header(birth_date="1990-01-01")}
    (res,) = run([entry(closed=date(2025, 3, 25))], {}, rows=rows, headers=hdr, cfg=cfg)
    assert res["line"] is None and "no inn" in res["reason"]


def test_populated_but_different_cabinet_inn_is_never_overridden(cfg):
    rows = {"395397": make_row(user_inn="2222222222")}
    (res,) = run([entry(closed=date(2025, 3, 25))], {}, rows=rows, cfg=cfg)
    assert res["line"] is None and "inn mismatch" in res["reason"]


def test_list_inn_contradicting_the_accepted_line_blocks(cfg):
    e = entry(inn="1111111111")
    (res,) = run([e], {"395397": history((2026, 8))}, cfg=cfg)
    assert res["line"] is None and "inn mismatch" in res["reason"]


# --- self-check -------------------------------------------------------------------

def test_self_check_catches_dlds_after_dldff(cfg):
    hdr = {"395397": header(applied_at="2025-05-01", payment_date="2025-06-01")}
    (res,) = run([entry(closed=date(2025, 3, 25))], {}, headers=hdr, cfg=cfg)
    assert res["line"] is None and res["reason"].startswith("self-check")


# --- history from the uploader DB ----------------------------------------------------

def test_read_history_prefers_accepted_lines(tmp_path):
    path = tmp_path / "u.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, raw_line TEXT, status TEXT, last_error TEXT)")
    rows = [(json.dumps(sent_subject(sent_deal(month=7)), ensure_ascii=False), "sent"),
            (json.dumps(sent_subject(sent_deal(month=8)), ensure_ascii=False), "sent"),
            (json.dumps(sent_subject(sent_deal(month=9)), ensure_ascii=False), "rejected"),
            (json.dumps(sent_subject(sent_deal("999")), ensure_ascii=False), "sent")]
    conn.executemany("INSERT INTO records (raw_line, status) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    hist = cl.read_history(str(path), {"395397", "555"})
    assert set(hist) == {"395397"}
    h = hist["395397"]
    assert h.accepted == {(2026, 7), (2026, 8)} and h.last_accepted == (2026, 8)
    assert h.deal["deallife"][0]["dlmonth"] == 8      # the rejected Sep line lost
    assert not h.final_accepted


def test_read_history_flags_an_accepted_final_status(tmp_path):
    path = tmp_path / "u.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, raw_line TEXT, status TEXT, last_error TEXT)")
    conn.execute("INSERT INTO records (raw_line, status) VALUES (?, 'sent')",
                 (json.dumps(sent_subject(sent_deal(flstat=2))),))
    conn.commit()
    conn.close()
    assert cl.read_history(str(path), {"395397"})["395397"].final_accepted


# --- canary and outputs ------------------------------------------------------------

def _res(dlref, group, status, closed="2026-08-01", line="x", cabinet_closed=True):
    return {"dlref": dlref, "group": group, "status": status, "closed": closed, "line": line,
            "cabinet_closed": cabinet_closed, "doc_complete": True}


def test_pick_canary_composition():
    results = [_res("9", "B", 12, cabinet_closed=False),
               _res("10", "B", 12), _res("11", "B", 12), _res("12", "B", 12),
               _res("20", "B", 2), _res("21", "B", 2), _res("22", "B", 2),
               _res("30", "D", 2, "2023-01-01"), _res("31", "D", 2, "2026-08-14"),
               dict(_res("32", "D", 2, "2026-09-01"), doc_complete=False),
               _res("40", "B", 2, line=None), _res("5", "B", 2, line=None)]
    assert cl.pick_canary(results) == {"10", "11", "20", "21", "31"}


def test_write_outputs_waves(tmp_path):
    results = [dict(_res("1", "B", 2, line='{"a":1}'), inn="", period="", last_accepted="",
                    reason="", list_inn_adopted=False),
               dict(_res("2", "B", 2, line='{"b":2}'), inn="", period="", last_accepted="",
                    reason="", list_inn_adopted=False),
               dict(_res("3", "C", 2, line=None), inn="", period="", last_accepted="",
                    reason="3004", list_inn_adopted=False)]
    txt, report = cl.write_outputs(results, "canary", {"1"}, tmp_path, TODAY)
    assert txt.name == "VigruzkaUBKI_CLOSE_2026-09-24_canary.txt"
    assert txt.read_text(encoding="utf-8") == '{"a":1}\n'
    txt, _ = cl.write_outputs(results, "rest", {"1"}, tmp_path, TODAY)
    assert txt.read_text(encoding="utf-8") == '{"b":2}\n'
    assert "3004" in report.read_text(encoding="utf-8")
    assert not list(tmp_path.glob(".*.tmp"))


def test_read_list_rejects_duplicates(tmp_path):
    path = tmp_path / "l.csv"
    path.write_text("dlref,inn,mark,closed\n1,2,,2026-08-01\n1,2,,2026-08-02\n", encoding="utf-8")
    try:
        cl.read_list(path)
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:
        raise AssertionError("duplicate dlref accepted")


# --- locked deals: deletion wave and unlocked closures ---------------------------------

def _db(tmp_path, rows):
    path = tmp_path / "u.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, raw_line TEXT, status TEXT,"
                 " last_error TEXT)")
    conn.executemany("INSERT INTO records (raw_line, status, last_error) VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return str(path)


def test_bureau_3004_on_our_last_attempt_locks_the_deal(tmp_path):
    line = json.dumps(sent_subject(sent_deal()), ensure_ascii=False)
    path = _db(tmp_path, [(line, "rejected", "main_errcode=3004; [CRITICAL 3004] ...")])
    hist = cl.read_history(path, {"395397"})["395397"]
    assert hist.bureau_3004 and cl.classify(entry(), hist) == "C"


def test_a_later_success_clears_the_bureau_lock(tmp_path):
    line = json.dumps(sent_subject(sent_deal()), ensure_ascii=False)
    path = _db(tmp_path, [(line, "rejected", "main_errcode=3004"), (line, "sent", None)])
    assert not cl.read_history(path, {"395397"})["395397"].bureau_3004


def test_locked_deal_keeps_its_closure_aside_until_unlocked(cfg):
    e = entry(closed=date(2026, 8, 25))
    hist = {"395397": history((2026, 9))}
    (locked,) = run([e], hist, cfg=cfg)
    assert locked["group"] == "C" and locked["line"] is None and locked["locked_line"]
    (free,) = cl.build_all([e], hist, {e.dlref: make_row()}, {e.dlref: header()}, cfg, TODAY,
                           unlocked={"395397"})
    assert free["unlocked"] and free["line"] == locked["locked_line"]
    s = only_slice(free)[2]
    assert (s["dlyear"], s["dlmonth"]) == (2026, 8) and s["dldff"] == "2026-08-25"


def test_delete_line_covers_every_month_after_the_closure(cfg):
    e = entry(closed=date(2025, 11, 10))
    (res,) = run([e], {"395397": history((2026, 9))}, cfg=cfg)
    pkg = json.loads(cl.delete_line(res["locked_line"], e, TODAY))
    assert not {"idents", "docs", "addrs", "contacts"} & set(pkg)
    assert pkg["inn"] == INN and pkg["person_id"] == "77"
    (deal,) = pkg["deals"]
    periods = [(s["dlyear"], s["dlmonth"]) for s in deal["deallife"]]
    assert periods[0] == (2025, 12) and periods[-1] == (2026, 9) and len(periods) == 10
    for s in deal["deallife"]:
        assert s["dlamtcur"] == s["dlamtexp"] == s["dldayexp"] == 0 and "dldff" not in s
        clc = date.fromisoformat(s["dldateclc"])
        assert (clc.year, clc.month) == (s["dlyear"], s["dlmonth"]) and clc <= TODAY
    assert deal["deallife"][-1]["dldateclc"] == TODAY.isoformat()


def test_read_unlocked_takes_only_accepted_deletions(tmp_path):
    log = tmp_path / "d.jsonl"
    log.write_text("\n".join(json.dumps(r) for r in [
        {"reqtype": "d", "state": "ok", "dlrefs": ["1"]},
        {"reqtype": "d", "state": "nt", "dlrefs": ["2"]},
        {"reqtype": "d", "state": "er", "dlrefs": ["3"]},
        {"reqtype": "d", "state": None, "dlrefs": ["4"]},
        {"reqtype": "u", "state": "ok", "dlrefs": ["5"]}]) + "\n", encoding="utf-8")
    assert cl.read_unlocked(log) == {"1", "2"}


def test_waves_keep_locked_closures_out_of_ordinary_files(cfg):
    locked, free = entry("1", closed=date(2026, 8, 25)), entry("2", closed=date(2026, 9, 10))
    hist = {"1": history((2026, 9), deal=sent_deal("1")), "2": history((2026, 9), deal=sent_deal("2"))}
    rows = {"1": make_row(app_id=1), "2": make_row(app_id=2)}
    hdrs = {"1": header("1"), "2": header("2")}
    res = cl.build_all([locked, free], hist, rows, hdrs, cfg, TODAY, unlocked={"1"})
    for wave in ("all", "rest"):
        assert [json.loads(x)["deals"][0]["dlref"] for x in cl.select_lines(res, wave, set(), TODAY)] == ["2"]
    assert [json.loads(x)["deals"][0]["dlref"] for x in cl.select_lines(res, "unlocked", set(), TODAY)] == ["1"]
    res = cl.build_all([locked, free], hist, rows, hdrs, cfg, TODAY)
    (d,) = cl.select_lines(res, "delete", set(), TODAY)
    assert json.loads(d)["deals"][0]["deallife"][0]["dlmonth"] == 9
    assert cl.select_lines(res, "delete", set(), TODAY, only={"2"}) == []


def test_deletion_file_is_not_picked_up_by_the_uploader_glob():
    assert cl.output_name("delete", TODAY).endswith(".jsonl")
    assert cl.output_name("unlocked", TODAY).endswith(".txt")


def test_bureau_2090_on_our_last_attempt_means_already_final(tmp_path):
    line = json.dumps(sent_subject(sent_deal()), ensure_ascii=False)
    path = _db(tmp_path, [(line, "sent", None), (line, "rejected", "main_errcode=2090; ...")])
    hist = cl.read_history(path, {"395397"})["395397"]
    assert hist.bureau_final and cl.classify(entry(), hist) == "A"
