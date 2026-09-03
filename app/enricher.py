"""Enrichment stage: producer files become full UBKI fo_cki subjects.

The producer delivers `.txt` JSONL lines carrying only `inn`, name, `bdate`
and `deals`+`deallife`. UBKI requires a complete subject (idents/docs/addrs/
contacts, person_id, is_gone, dlvidobes), so this stage runs before the
uploader (cron 05:30 vs 06:00):

    RAW_FOLDER (producer drops here)
        -> enrich (joins the cabinet MySQL: dlref = applications.id)
        -> UBKI_DATA_FOLDER_PATH (uploader inbox, new file, same name)
        -> quarantine/<same name> for lines that can't be enriched
        -> processed/ for consumed raw files

Unlike the uploader (which must treat lines as opaque bytes), the enricher
legitimately parses them. Identity blocks come from the NEWEST application's
snapshot among the line's deals (`vdate` = applied_at) with `users` as
fallback; deal fields pass through as-is, only missing mandatory `dlvidobes`
is injected. The document issuer (`dwho`) has a third source: when neither the
snapshot nor `users` carries it, it is taken from another application of the
SAME client that states an issuer for the SAME document number (see
`_find_peer_document`); if that fails too, the document is sent incomplete
(the bureau drops it and keeps the deal data — see `build_doc`) and counted in
`summary.lines_doc_incomplete`. `csex` and the birth-date check are derived
from the tax id itself (`_inn_sex` / `_inn_birthday`), which the cabinet
cannot provide. A line is quarantined when: broken JSON, no/unknown dlref,
deals of different clients, file inn != users.social_number, bdate
contradicting the date encoded in the inn (never risk writing someone else's
credit history), no passport number at all, unsupported passport format,
no valid phone.
Quarantine records are {"line_no", "reason", "line"}; drop the fixed file
back into RAW_FOLDER to reprocess (the wrapper is recognized and unwrapped).

Idempotency: file identity = filename + sha256 in the `enriched_files` table.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from sqlite3 import Connection
from typing import Callable

from . import db
from .alerts import send_telegram
from .config import Config
from .uploader import scan_folder, sha256_of, unique_target

log = logging.getLogger("ubki.enricher")

CITIZENSHIP_UKRAINE = "804"   # dir.4 / ISO 3166 numeric
ADDR_TYPE_ACTUAL = "2"        # dir.9
CONTACT_TYPE_MOBILE = "3"     # dir.10
LANG_UKRAINIAN = "1"          # dir.23
DOC_TYPE_PASSPORT = "1"       # dir.7: passport book (2 letters + 6 digits)
DOC_TYPE_ID_CARD = "17"       # dir.7: ID card (9 digits); eddr unknown in DB (v1 sends without)
SEX_MALE = "1"                # dir.1
SEX_FEMALE = "2"              # dir.1

_QUARANTINE_KEYS = {"line_no", "reason", "line"}

# Row shape expected from the fetcher (per application id):
FETCH_SQL = """
SELECT a.id, a.user_id, a.applied_at,
       a.passport_number  AS snap_passport_number,
       a.passport_date    AS snap_passport_date,
       a.passport_issued_by AS snap_passport_issued_by,
       a.phone_mobile     AS snap_phone,
       a.addr_postcode, a.addr_city, a.addr_street,
       a.addr_house, a.addr_building, a.addr_flat,
       u.social_number    AS user_inn,
       u.phone            AS user_phone,
       u.passport_number  AS user_passport_number,
       u.passport_date    AS user_passport_date,
       u.passport_issued_by AS user_passport_issued_by
FROM finplugs_creditup_applications a
JOIN users u ON u.id = a.user_id
WHERE a.id IN ({placeholders})
"""

# Applications of a client that DO carry a document issuer — the fallback source
# for lines whose own application left `passport_issued_by` empty. Only the few
# columns needed to match the document and complete it.
FETCH_ISSUERS_SQL = """
SELECT user_id, passport_number, passport_issued_by, passport_date, applied_at
FROM finplugs_creditup_applications
WHERE user_id IN ({placeholders})
  AND passport_issued_by IS NOT NULL AND passport_issued_by <> ''
"""

# user_ids per query: the applications table has ~19KB average rows, so this is
# kept small deliberately — a wide IN list would read gigabytes off disk.
ISSUER_CHUNK = 500

Fetcher = Callable[[Config, list[str]], dict[str, dict]]
IssuerFetcher = Callable[[Config, list[int]], dict[int, list[dict]]]


@dataclass
class EnrichSummary:
    dry_run: bool = False
    files_seen: int = 0
    files_processed: int = 0
    files_skipped: int = 0  # outside FILE_GLOB (filled by scan_folder)
    files_empty: int = 0    # raw files with zero data lines (truncated export)
    lines_total: int = 0
    lines_enriched: int = 0
    lines_quarantined: int = 0
    # enriched lines whose `dwho` came from another application of the same
    # client (would have been quarantined before)
    lines_issuer_from_peer: int = 0
    # enriched lines whose document went out INCOMPLETE (no `dwho` and/or no
    # `dwdt`): the bureau drops such a doc (IGNORED 3003) but records the deal
    # data, which is why these are sent instead of quarantined. Counted here
    # because that is the only remaining visibility into the cabinet gap.
    lines_doc_incomplete: int = 0
    quarantine_reasons: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return self.__dict__ | {
            "quarantine_reasons": list(self.quarantine_reasons),
            "errors": list(self.errors),
        }


def _open_cabinet(config: Config):
    """pymysql is imported lazily so the api/uploader never need it at import
    time (only `app.enrich` talks to the cabinet)."""
    import pymysql

    return pymysql.connect(
        host=config.mysql_host, port=config.mysql_port,
        user=config.mysql_user, password=config.mysql_password,
        database=config.mysql_db, connect_timeout=10, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def fetch_deals_data(config: Config, dlrefs: list[str]) -> dict[str, dict]:
    """One batch query per file; returns {dlref: row}."""
    if not dlrefs:
        return {}
    conn = _open_cabinet(config)
    try:
        placeholders = ", ".join(["%s"] * len(dlrefs))
        with conn.cursor() as cur:
            cur.execute(FETCH_SQL.format(placeholders=placeholders), dlrefs)
            return {str(row["id"]): row for row in cur.fetchall()}
    finally:
        conn.close()


def fetch_passport_issuers(config: Config, user_ids: list[int]) -> dict[int, list[dict]]:
    """Other applications of these clients that carry a document issuer, keyed by
    user_id. Queried in chunks and only for the clients whose own application
    left `dwho` empty, so the extra read stays proportional to the problem."""
    if not user_ids:
        return {}
    out: dict[int, list[dict]] = {}
    conn = _open_cabinet(config)
    try:
        with conn.cursor() as cur:
            for start in range(0, len(user_ids), ISSUER_CHUNK):
                chunk = user_ids[start:start + ISSUER_CHUNK]
                placeholders = ", ".join(["%s"] * len(chunk))
                cur.execute(FETCH_ISSUERS_SQL.format(placeholders=placeholders), chunk)
                for row in cur.fetchall():
                    out.setdefault(row["user_id"], []).append(row)
    finally:
        conn.close()
    return out


# --- pure building blocks --------------------------------------------------

def normalize_phone(value) -> str | None:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 12 and digits.startswith("380"):
        return f"+{digits}"
    if len(digits) == 10 and digits.startswith("0"):
        return f"+38{digits}"
    return None


def _inn_birthday(inn: str) -> str | None:
    """Birth date encoded in a Ukrainian tax id: the first five digits are the
    number of days since 1899-12-31. Returns None for anything that is not a
    10-digit id. Verified against the 2026-08-14 prod file: the derived date
    matched the producer's `bdate` on 63615 of 63644 lines (99.95%)."""
    if not (inn.isdigit() and len(inn) == 10):
        return None
    try:
        return (date(1899, 12, 31) + timedelta(days=int(inn[:5]))).isoformat()
    except (ValueError, OverflowError):
        return None


def _inn_sex(inn: str) -> str | None:
    """`csex` (dir.1: 1=Чоловік, 2=Жінка) from the 9th digit of the tax id —
    odd = male, even = female. Cross-checked against patronymic endings on the
    2026-08-14 prod file: agreed on 62968 of 63069 lines (99.84%), the residue
    being unusual patronymics rather than mismatched ids. The cabinet has no
    usable sex column, and without csex UBKI attaches NOTICE 4014 to EVERY
    record, which is what saturated the warning alert."""
    if not (inn.isdigit() and len(inn) == 10):
        return None
    return SEX_MALE if int(inn[8]) % 2 else SEX_FEMALE


def _iso_date(value) -> str | None:
    """MySQL drivers return date/datetime objects; quarantine formats we don't
    recognize — including calendar-invalid strings like MySQL's zero-date
    '0000-00-00' or '2021-02-30' — rather than sending garbage or letting them
    reach _plus_years, where date.fromisoformat would raise and abort the run."""
    if value is None or value == "":
        return None
    if hasattr(value, "date"):  # datetime
        value = value.date()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _sort_key(value) -> str:
    """Full-resolution key for picking the NEWEST application. Unlike _iso_date
    (date-only), this keeps the time component so two same-day applications
    don't tie and silently fall back to the line's deal order. ISO strings sort
    chronologically; a missing value sorts first (oldest)."""
    if value is None or value == "":
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value).strip()


def _plus_years(iso_day: str, years: int) -> str:
    day = date.fromisoformat(iso_day)
    try:
        return day.replace(year=day.year + years).isoformat()
    except ValueError:  # Feb 29 -> Feb 28
        return day.replace(year=day.year + years, day=28).isoformat()


def _normalize_number(value) -> str:
    """Passport numbers arrive with stray spaces and mixed case across sources."""
    return re.sub(r"\s+", "", str(value or "")).upper()


def _parse_doc_number(value) -> tuple[str, str, str] | None:
    """(dtype, dser, dnom) for a document number in a supported format, else
    None: 9 digits = ID card (dir.7 code 17), 2 non-digits + 6 digits = passport
    book (code 1). Shared by `build_doc` and `_clients_missing_issuer` so both
    agree on what counts as a usable document."""
    number = _normalize_number(value)
    if not number:
        return None
    if re.fullmatch(r"\d{9}", number):
        return DOC_TYPE_ID_CARD, "", number
    if (len(number) == 8 and number[2:].isdigit()
            and not any(ch.isdigit() for ch in number[:2])):
        return DOC_TYPE_PASSPORT, number[:2], number[2:]
    return None


def _find_peer_document(peers: list[dict] | None, number: str) -> dict | None:
    """Issuer (and issue date) that the SAME client stated for the SAME document
    number in another application. Matching on the number is what keeps this
    factual: an issuer belonging to a different passport must never be attached
    to this one — we complete missing data, we never invent it. The newest
    application wins, so the ordering is enforced here instead of being trusted
    from the fetcher."""
    candidates = [
        peer for peer in (peers or ())
        if _normalize_number(peer.get("passport_number")) == number
        and str(peer.get("passport_issued_by") or "").strip()
    ]
    if not candidates:
        return None
    newest = max(candidates, key=lambda peer: _sort_key(peer.get("applied_at")))
    return {
        "issuer": str(newest["passport_issued_by"]).strip(),
        "issued_at": _iso_date(newest.get("passport_date")),
    }


def _doc_fields(vdate: str, dtype: str, dser: str, dnom: str,
                issued_by: str, issued_at: str | None) -> dict:
    """One `docs[]` entry from whatever the cabinet actually has. Empty fields
    are omitted rather than sent blank — the bureau treats the two identically
    (a missing key answers with the same IGNORED 3003 as an empty value), and
    omitting keeps us from asserting a fact we do not hold."""
    doc = {"vdate": vdate, "lng": LANG_UKRAINIAN,
           "dtype": dtype, "dser": dser, "dnom": dnom}
    if issued_by:
        doc["dwho"] = issued_by
    if issued_at:
        doc["dwdt"] = issued_at
        if dtype == DOC_TYPE_ID_CARD:
            # the cabinet has no expiry field, so dterm is derived as issue
            # date + 10 years (the statutory adult ID-card validity)
            doc["dterm"] = _plus_years(issued_at, 10)
    return doc


def build_doc(row: dict, vdate: str, peers: list[dict] | None = None,
              stats: dict | None = None) -> tuple[dict | None, str | None]:
    """docs[0] from the application snapshot, falling back to users and then to
    the client's other applications (`peers`, matched by document number).
    Format detection: 2 letters + 6 digits = passport book (dtype 1),
    9 digits = ID card (dtype 17, sent without eddr_number — the bureau never
    asked for it live).

    A document is COMPLETE when it carries both an issuer (`dwho`) and an issue
    date (`dwdt`); anything less the bureau throws away with IGNORED 3003, so a
    source holding both is always preferred over one holding part.

    An incomplete document is still emitted, NOT quarantined. Measured on prod
    2026-09-03 (`probe_dwho.py`, 300 of the 7373 issuer-less lines): none of
    them drew 2077, i.e. the bureau already holds a document for that whole
    cohort, so a package without an issuer is accepted with the doc merely
    dropped — and blocking those lines locally threw away deliverable deal data
    for nothing. `stats` records which path was taken so the caller can count
    peer recoveries and dropped-doc lines only for lines that fully enrich."""
    partial: dict | None = None  # best doc we can emit; the bureau will drop it
    saw_any_number = False
    for prefix in ("snap", "user"):
        number = _normalize_number(row.get(f"{prefix}_passport_number"))
        if not number:
            continue
        saw_any_number = True
        parsed = _parse_doc_number(number)
        if parsed is None:
            continue  # unsupported format in this source; try the other one
        dtype, dser, dnom = parsed
        issued_by = str(row.get(f"{prefix}_passport_issued_by") or "").strip()
        issued_at = _iso_date(row.get(f"{prefix}_passport_date"))
        used_peer = False
        if not issued_by:
            # this application left the issuer empty, but the client stated it
            # for the same document elsewhere — complete it from there
            peer = _find_peer_document(peers, number)
            if peer is not None:
                issued_by = peer["issuer"]
                issued_at = issued_at or peer["issued_at"]
                used_peer = True
        doc = _doc_fields(vdate, dtype, dser, dnom, issued_by, issued_at)
        if not (issued_by and issued_at):
            # keep the snapshot's partial over users' one, and keep looking:
            # the other source may still hold a complete document. A peer that
            # supplied an issuer but no date lands here too — the bureau drops
            # that doc all the same, so it is counted as dropped, not as a
            # recovery, to keep `lines_issuer_from_peer` honest.
            partial = partial or doc
            continue
        if used_peer and stats is not None:
            stats["issuer_from_peer"] = True
        return doc, None
    if partial is not None:
        if stats is not None:
            stats["doc_incomplete"] = True
        return partial, None
    if saw_any_number:
        # a number exists but neither source could be parsed: this is a format
        # question (extend the parser or fix the cabinet value), unlike the case
        # below, which is a plain data gap — reporting both as "unsupported
        # format" hid which of the two an operator was looking at
        return None, "unsupported passport format (neither 2 letters + 6 digits nor 9 digits)"
    return None, ("no passport number in the cabinet"
                  " (both the application snapshot and users are empty)")


def build_addr(row: dict, vdate: str) -> dict:
    addr = {"vdate": vdate, "lng": LANG_UKRAINIAN,
            "adtype": ADDR_TYPE_ACTUAL, "adcountry": CITIZENSHIP_UKRAINE}
    for target, source in (
        ("adindex", "addr_postcode"), ("adcity", "addr_city"),
        ("adstreet", "addr_street"), ("adhome", "addr_house"),
        ("adcorp", "addr_building"), ("adflat", "addr_flat"),
    ):
        value = str(row.get(source) or "").strip()
        if value:
            addr[target] = value
    return addr


def unwrap_quarantine(obj):
    """A re-dropped quarantine record carries the original line under `line`."""
    if isinstance(obj, dict) and set(obj.keys()) == _QUARANTINE_KEYS:
        inner = obj["line"]
        if isinstance(inner, str):
            return json.loads(inner)
        return inner
    return obj


def enrich_line(line_obj: dict, rows: dict[str, dict], config: Config,
                issuer_peers: dict[int, list[dict]] | None = None,
                summary: EnrichSummary | None = None) -> tuple[dict | None, str | None]:
    """Build the full fo_cki subject, or return (None, reason) for quarantine.
    `issuer_peers` (keyed by user_id) is the third `dwho` source; `summary`, when
    given, counts lines whose document was completed from it."""
    inn = str(line_obj.get("inn") or "").strip()
    if not inn:
        return None, "line has no inn"
    deals = line_obj.get("deals")
    if not isinstance(deals, list) or not deals:
        return None, "line has no deals"

    deal_rows = []
    for deal in deals:
        dlref = str(deal.get("dlref") or "").strip() if isinstance(deal, dict) else ""
        if not dlref:
            return None, "deal has no dlref"
        row = rows.get(dlref)
        if row is None:
            return None, f"dlref {dlref} not found in cabinet DB"
        deal_rows.append(row)

    user_ids = {row["user_id"] for row in deal_rows}
    if len(user_ids) > 1:
        return None, f"deals belong to different clients: user_ids={sorted(user_ids)}"

    # full-resolution ordering so same-day applications don't tie (see _sort_key)
    newest = max(deal_rows, key=lambda row: _sort_key(row.get("applied_at")))
    user_inn = str(newest.get("user_inn") or "").strip()
    if user_inn != inn:
        # Both cases block (we never write a credit history under an unverified
        # tax id), but they are DIFFERENT problems and must not be reported as
        # one: an empty `users.social_number` is a cabinet data gap — fill it and
        # the line passes — while a populated-but-different inn is an identity
        # conflict needing investigation. Measured on the 2026-08-14 file: 165 of
        # 174 were the empty case, all of them mislabelled as "different inn".
        if not user_inn:
            return None, (f"cabinet client has no inn (users.social_number is empty),"
                          f" file has {inn}")
        return None, f"inn mismatch: file has {inn}, cabinet client has different inn"

    vdate = _iso_date(newest.get("applied_at"))
    if vdate is None:
        return None, "application has no applied_at (vdate)"

    doc_stats: dict = {}
    doc, doc_reason = build_doc(
        newest, vdate, (issuer_peers or {}).get(newest["user_id"]), doc_stats)
    if doc is None:
        return None, doc_reason

    phone = normalize_phone(newest.get("snap_phone")) or normalize_phone(newest.get("user_phone"))
    if phone is None:
        return None, "no valid phone (contacts block is mandatory)"

    lname = str(line_obj.get("lname") or "").strip()
    fname = str(line_obj.get("fname") or "").strip()
    bdate = str(line_obj.get("bdate") or "").strip()
    if not (lname and fname and bdate):
        return None, "line is missing lname/fname/bdate"
    mname = str(line_obj.get("mname") or "").strip()

    # The tax id encodes the birth date, so a contradiction means one of the two
    # is wrong and we must not write it into someone's credit history — the same
    # guardrail as the inn check above. The bureau catches it anyway (CRITICAL
    # 2098 on 21 of the 29 contradicting lines of the 2026-08-14 file, the other
    # 8 slipped through), and a quarantine record names both values instead.
    # Only ISO dates are compared: a format we don't recognize would produce
    # false positives, and it is not this check's job to police the format.
    inn_bdate = _inn_birthday(inn)
    if inn_bdate and re.fullmatch(r"\d{4}-\d{2}-\d{2}", bdate[:10]) and bdate[:10] != inn_bdate:
        return None, (f"bdate {bdate} contradicts the date encoded in inn {inn}"
                      f" (expected {inn_bdate})")

    ident = {"vdate": vdate, "lng": LANG_UKRAINIAN, "inn": inn,
             "lname": lname, "fname": fname}
    if mname:
        ident["mname"] = mname
    ident |= {"bdate": bdate, "cgrag": CITIZENSHIP_UKRAINE}
    # csex is optional per the spec, but omitting it makes the bureau attach
    # NOTICE 4014 to every single record (see _inn_sex)
    csex = _inn_sex(inn)
    if csex:
        ident["csex"] = csex

    # deals pass through as-is; only the missing mandatory dlvidobes is injected
    out_deals = []
    for deal in deals:
        deal = dict(deal)
        deal.setdefault("dlvidobes", config.deal_vidobes)
        out_deals.append(deal)

    subject = {"reqlng": str(line_obj.get("reqlng") or LANG_UKRAINIAN),
               "inn": inn,
               "person_id": str(newest["user_id"]),
               "is_gone": "0",
               "lname": lname, "fname": fname}
    if mname:
        subject["mname"] = mname
    subject |= {
        "bdate": bdate,
        "idents": [ident],
        "docs": [doc],
        "addrs": [build_addr(newest, vdate)],
        "deals": out_deals,
        "contacts": [{"vdate": vdate, "ctype": CONTACT_TYPE_MOBILE, "cval": phone}],
    }
    # counted only here: a doc completed from a peer whose line still failed
    # later (no phone, missing name) is not a recovery
    if summary is not None and doc_stats.get("issuer_from_peer"):
        summary.lines_issuer_from_peer += 1
    if summary is not None and doc_stats.get("doc_incomplete"):
        summary.lines_doc_incomplete += 1
    return subject, None


# --- file processing ---------------------------------------------------------

_FALLBACK_ENCODINGS = ("cp1251",)  # producer occasionally exports Cyrillic fields in Windows codepage


def _read_numbered_lines(path: Path) -> list[tuple[int, str]]:
    """Non-blank lines paired with their TRUE 1-based line number in the file,
    so a quarantine record points the operator at the right line even when the
    raw file has blank lines (read_lines drops blanks and loses the mapping)."""
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        for encoding in _FALLBACK_ENCODINGS:
            try:
                text = raw.decode(encoding)
            except UnicodeDecodeError:
                continue
            log.warning("raw file not UTF-8, decoded with fallback encoding", extra={
                "event": "raw_file_fallback_encoding", "file": path.name, "encoding": encoding})
            break
        else:
            raise
    lines = text.splitlines()
    return [(n, line) for n, line in enumerate(lines, start=1) if line.strip()]


def _write_enriched(folder: Path, name: str, sha256: str, lines: list[str]) -> Path:
    """Atomically write the enriched inbox file. The raw->enriched transform is
    deterministic, so a byte-identical file already under this name is a prior
    crashed attempt for the same raw (killed between the write and the
    idempotency-row commit) — reuse it instead of emitting a second same-content
    file under a prefixed name, which the uploader would ingest as a distinct
    identity and send to UBKI twice. A DIFFERENT file already holding the name
    (an earlier, still-unconsumed enriched file) is preserved via the sha prefix."""
    data = ("\n".join(lines) + "\n").encode("utf-8")
    target = folder / name
    if target.exists() and target.read_bytes() != data:
        target = folder / f"{sha256[:8]}_{name}"
    if target.exists() and target.read_bytes() == data:
        return target
    tmp = target.with_name(f".{target.name}.tmp")  # hidden: invisible to the uploader scan
    tmp.write_bytes(data)
    tmp.rename(target)
    return target


def _clients_missing_issuer(rows: dict[str, dict]) -> list[int]:
    """Clients `build_doc` would need a peer application for: some source
    carries a document number in a supported format but no issuer, and no
    source carries one on its own (a peer can only supply an issuer, so a
    source that already has one is past this lookup).

    Judging by the document format (instead of "both issuers are empty") closes
    two holes at once: a client whose issuer sits only on a source with an
    unusable number was never looked up although its other source needed one,
    while clients with no usable number anywhere were looked up in vain — they
    are quarantined on the format regardless of the issuer."""
    need: set[int] = set()
    for row in rows.values():
        complete = missing_issuer = False
        for prefix in ("snap", "user"):
            if _parse_doc_number(row.get(f"{prefix}_passport_number")) is None:
                continue
            if str(row.get(f"{prefix}_passport_issued_by") or "").strip():
                complete = True
            else:
                missing_issuer = True
        if missing_issuer and not complete:
            need.add(row["user_id"])
    return sorted(need)


def process_file(conn: Connection, config: Config, path: Path,
                 summary: EnrichSummary, fetch: Fetcher, dry_run: bool,
                 fetch_issuers: IssuerFetcher | None = None) -> None:
    sha = sha256_of(path)
    if db.get_enriched_by_identity(conn, path.name, sha):
        return  # already enriched (identity = filename + sha256)

    numbered = _read_numbered_lines(path)
    summary.files_processed += 1
    summary.lines_total += len(numbered)
    log.info("raw file discovered", extra={
        "event": "raw_file_new", "file": path.name, "sha256": sha, "lines": len(numbered)})
    if not numbered:
        # truncated/blank producer export: no enriched output would ever reach
        # the uploader, so surface it here or it vanishes silently
        summary.files_empty += 1
        log.warning("raw file has no data lines", extra={
            "event": "raw_file_empty", "file": path.name, "sha256": sha})
    if dry_run:
        return

    parsed: list[tuple[int, str, dict | None, str | None]] = []
    dlrefs: set[str] = set()
    for line_no, raw in numbered:
        try:
            obj = unwrap_quarantine(json.loads(raw))
        except ValueError as exc:
            parsed.append((line_no, raw, None, f"broken JSON: {exc}"))
            continue
        if not isinstance(obj, dict):
            parsed.append((line_no, raw, None, "line is not a JSON object"))
            continue
        for deal in obj.get("deals") or []:
            if isinstance(deal, dict) and deal.get("dlref"):
                dlrefs.add(str(deal["dlref"]))
        parsed.append((line_no, raw, obj, None))

    rows = fetch(config, sorted(dlrefs)) if dlrefs else {}

    # a large share of applications leave `dwho` empty in both sources (the
    # cabinet stopped collecting it in 2020). The same client sometimes stated
    # it in another application, so fetch those as a fallback —
    # scoped to the affected clients only, since the applications table has
    # ~19KB rows and a wider query would read gigabytes.
    need_issuer = _clients_missing_issuer(rows)
    peers: dict[int, list[dict]] = {}
    if need_issuer:
        peers = (fetch_issuers or fetch_passport_issuers)(config, need_issuer)
        log.info("issuer peer lookup", extra={
            "event": "issuer_peer_lookup", "file": path.name,
            "clients_missing_issuer": len(need_issuer),
            "clients_with_candidates": len(peers)})

    enriched: list[str] = []
    quarantined: list[dict] = []
    for line_no, raw, obj, parse_error in parsed:
        reason = parse_error
        if reason is None:
            subject, reason = enrich_line(obj, rows, config, peers, summary)
            if reason is None:
                enriched.append(json.dumps(subject, ensure_ascii=False, separators=(",", ":")))
        if reason is not None:
            quarantined.append({"line_no": line_no, "reason": reason, "line": raw})
            log.warning("line quarantined", extra={
                "event": "line_quarantined", "file": path.name,
                "line_no": line_no, "reason": reason})

    if enriched:
        config.data_folder.mkdir(parents=True, exist_ok=True)
        target = _write_enriched(config.data_folder, path.name, sha, enriched)
        # atomic rename means the file can never be seen half-written, so the
        # uploader's mtime freshness guard is pointless here — backdate it so
        # a manual enrich -> run_once chain works without waiting
        backdated = time.time() - config.min_file_age_sec - 1
        os.utime(target, (backdated, backdated))
        log.info("enriched file written", extra={
            "event": "file_enriched", "file": path.name,
            "target": str(target), "lines": len(enriched)})
    if quarantined:
        config.quarantine_folder.mkdir(parents=True, exist_ok=True)
        # same filename as the source: dropping it back into RAW_FOLDER for a
        # re-run works without renaming (it still matches FILE_GLOB). Written
        # reuse-if-identical + atomically (like _write_enriched) so a crash
        # between this write and the enriched_files idempotency row committed
        # below cannot leave a DUPLICATE quarantine file on reprocess: the
        # raw->quarantine transform is deterministic, so a byte-identical file
        # already under this name is our own prior attempt — reuse it. A
        # different file under the name (an earlier, unconsumed quarantine) is
        # preserved via the sha prefix.
        data = (
            "\n".join(json.dumps(record, ensure_ascii=False) for record in quarantined) + "\n"
        ).encode("utf-8")
        qtarget = config.quarantine_folder / path.name
        if qtarget.exists() and qtarget.read_bytes() != data:
            qtarget = unique_target(config.quarantine_folder, path.name, sha)
        if not (qtarget.exists() and qtarget.read_bytes() == data):
            tmp = qtarget.with_name(f".{qtarget.name}.tmp")
            tmp.write_bytes(data)
            tmp.rename(qtarget)
        log.warning("quarantine written", extra={
            "event": "file_quarantine", "file": path.name,
            "target": str(qtarget), "count": len(quarantined)})

    config.processed_folder.mkdir(parents=True, exist_ok=True)
    path.rename(unique_target(config.processed_folder, path.name, sha))

    db.insert_enriched_file(conn, path.name, sha, len(numbered), len(enriched), len(quarantined))
    summary.lines_enriched += len(enriched)
    summary.lines_quarantined += len(quarantined)
    summary.quarantine_reasons.extend(
        f"{path.name}:{record['line_no']}: {record['reason']}" for record in quarantined)


def build_alert(summary: EnrichSummary) -> str | None:
    if not (summary.lines_quarantined or summary.files_skipped
            or summary.files_empty or summary.errors
            or summary.lines_doc_incomplete):
        return None
    lines = ["UBKI enricher: проблеми при збагаченні"]
    if summary.lines_quarantined:
        lines.append(f"у карантині: {summary.lines_quarantined} рядк.")
        lines.extend(summary.quarantine_reasons[:5])
        if len(summary.quarantine_reasons) > 5:
            lines.append(f"… та ще {len(summary.quarantine_reasons) - 5}")
    if summary.lines_doc_incomplete:
        lines.append("документ без видавця/дати видачі (бюро його відкине, дані угоди"
                     f" дійдуть): {summary.lines_doc_incomplete} рядк.")
    if summary.files_skipped:
        lines.append(f"файлів у папці поза маскою FILE_GLOB: {summary.files_skipped}")
    if summary.files_empty:
        lines.append(f"порожніх raw-файлів (0 рядків даних): {summary.files_empty}")
    lines.extend(summary.errors)
    lines.append(f"збагачено: {summary.lines_enriched}")
    return "\n".join(lines)


def run_enrich(config: Config, fetch: Fetcher | None = None, dry_run: bool = False,
               fetch_issuers: IssuerFetcher | None = None) -> EnrichSummary:
    summary = EnrichSummary(dry_run=dry_run)
    fetch = fetch or fetch_deals_data
    fetch_issuers = fetch_issuers or fetch_passport_issuers
    config.enrich_lock_path.parent.mkdir(parents=True, exist_ok=True)
    with config.enrich_lock_path.open("w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log.warning("another enrich run is active, exiting", extra={"event": "enrich_lock_busy"})
            return summary

        if not dry_run:
            # the uploader inbox is a subfolder of the mount — make sure it
            # exists even before the first enriched file (06:00 pass must not
            # crash on a missing folder)
            config.data_folder.mkdir(parents=True, exist_ok=True)
        conn = db.connect(config.db_path)
        try:
            paths = scan_folder(config, summary, folder=config.raw_folder)
            summary.files_seen = len(paths)
            log.info("enrich started", extra={
                "event": "enrich_start", "files_seen": len(paths), "dry_run": dry_run})
            for path in paths:
                try:
                    process_file(conn, config, path, summary, fetch, dry_run, fetch_issuers)
                except Exception as exc:
                    # one bad file must not abort the whole batch; record it and
                    # move on so the rest of the day's files still get enriched
                    # (they are retried next run — partial state is idempotent)
                    summary.errors.append(f"{path.name}: {exc}")
                    log.exception("file enrichment failed", extra={
                        "event": "enrich_file_error", "file": path.name})
            log.info("enrich finished", extra={"event": "enrich_done", **summary.as_dict()})
        except Exception as exc:
            summary.errors.append(str(exc))
            log.exception("enrich crashed", extra={"event": "enrich_error"})
            send_telegram(config, f"UBKI enricher: збагачення впало з помилкою\n{exc}")
            raise
        else:
            alert = build_alert(summary)
            if alert:
                send_telegram(config, alert)
        finally:
            conn.close()
    return summary
