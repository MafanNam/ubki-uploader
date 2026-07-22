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
is injected. A line is quarantined when: broken JSON, no/unknown dlref, deals
of different clients, file inn != users.social_number (never risk writing
someone else's credit history), unsupported passport format, no valid phone.
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
from datetime import date
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

Fetcher = Callable[[Config, list[str]], dict[str, dict]]


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
    quarantine_reasons: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return self.__dict__ | {
            "quarantine_reasons": list(self.quarantine_reasons),
            "errors": list(self.errors),
        }


def fetch_deals_data(config: Config, dlrefs: list[str]) -> dict[str, dict]:
    """One batch query per file; returns {dlref: row}. pymysql is imported
    lazily so the api/uploader never need it at import time."""
    import pymysql

    if not dlrefs:
        return {}
    conn = pymysql.connect(
        host=config.mysql_host, port=config.mysql_port,
        user=config.mysql_user, password=config.mysql_password,
        database=config.mysql_db, connect_timeout=10, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        placeholders = ", ".join(["%s"] * len(dlrefs))
        with conn.cursor() as cur:
            cur.execute(FETCH_SQL.format(placeholders=placeholders), dlrefs)
            return {str(row["id"]): row for row in cur.fetchall()}
    finally:
        conn.close()


# --- pure building blocks --------------------------------------------------

def normalize_phone(value) -> str | None:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 12 and digits.startswith("380"):
        return f"+{digits}"
    if len(digits) == 10 and digits.startswith("0"):
        return f"+38{digits}"
    return None


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


def build_doc(row: dict, vdate: str) -> tuple[dict | None, str | None]:
    """docs[0] from the application snapshot, falling back to users.
    Format detection: 2 letters + 6 digits = passport book (dtype 1),
    9 digits = ID card (dtype 17, sent without eddr_number — the bureau never
    asked for it live). De-facto mandatory (live-confirmed IGNORED 3003 →
    CRITICAL 2077): `dwho` for every doc, `dterm` for ID cards. The cabinet
    has no expiry field, so dterm is derived as issue date + 10 years (the
    statutory adult ID-card validity)."""
    saw_valid_number = False
    saw_idcard_without_date = False
    for prefix in ("snap", "user"):
        number = re.sub(r"\s+", "", str(row.get(f"{prefix}_passport_number") or "")).upper()
        if not number:
            continue
        if re.fullmatch(r"\d{9}", number):
            dtype, dser, dnom = DOC_TYPE_ID_CARD, "", number
        elif (len(number) == 8 and number[2:].isdigit()
              and not any(ch.isdigit() for ch in number[:2])):
            dtype, dser, dnom = DOC_TYPE_PASSPORT, number[:2], number[2:]
        else:
            continue  # unsupported format in this source; try the other one
        saw_valid_number = True
        issued_by = str(row.get(f"{prefix}_passport_issued_by") or "").strip()
        if not issued_by:
            continue  # bureau drops docs without an issuer; try the other source
        issued_at = _iso_date(row.get(f"{prefix}_passport_date"))
        if dtype == DOC_TYPE_ID_CARD and not issued_at:
            saw_idcard_without_date = True
            continue  # dterm cannot be derived without the issue date
        doc = {"vdate": vdate, "lng": LANG_UKRAINIAN, "dtype": dtype, "dser": dser,
               "dnom": dnom, "dwho": issued_by}
        if issued_at:
            doc["dwdt"] = issued_at
        if dtype == DOC_TYPE_ID_CARD:
            doc["dterm"] = _plus_years(issued_at, 10)
        return doc, None
    if saw_idcard_without_date:
        return None, "ID-card has no issue date in the cabinet — dterm cannot be derived"
    if saw_valid_number:
        return None, "document issuer (dwho) is empty in the cabinet (bureau drops such docs, 3003)"
    return None, "unsupported passport format (neither 2 letters + 6 digits nor 9 digits)"


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


def enrich_line(line_obj: dict, rows: dict[str, dict], config: Config) -> tuple[dict | None, str | None]:
    """Build the full fo_cki subject, or return (None, reason) for quarantine."""
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
        return None, f"inn mismatch: file has {inn}, cabinet client has different inn"

    vdate = _iso_date(newest.get("applied_at"))
    if vdate is None:
        return None, "application has no applied_at (vdate)"

    doc, doc_reason = build_doc(newest, vdate)
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

    ident = {"vdate": vdate, "lng": LANG_UKRAINIAN, "inn": inn,
             "lname": lname, "fname": fname}
    if mname:
        ident["mname"] = mname
    ident |= {"bdate": bdate, "cgrag": CITIZENSHIP_UKRAINE}

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


def process_file(conn: Connection, config: Config, path: Path,
                 summary: EnrichSummary, fetch: Fetcher, dry_run: bool) -> None:
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

    enriched: list[str] = []
    quarantined: list[dict] = []
    for line_no, raw, obj, parse_error in parsed:
        reason = parse_error
        if reason is None:
            subject, reason = enrich_line(obj, rows, config)
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
            or summary.files_empty or summary.errors):
        return None
    lines = ["UBKI enricher: проблеми при збагаченні"]
    if summary.lines_quarantined:
        lines.append(f"у карантині: {summary.lines_quarantined} рядк.")
        lines.extend(summary.quarantine_reasons[:5])
        if len(summary.quarantine_reasons) > 5:
            lines.append(f"… та ще {len(summary.quarantine_reasons) - 5}")
    if summary.files_skipped:
        lines.append(f"файлів у папці поза маскою FILE_GLOB: {summary.files_skipped}")
    if summary.files_empty:
        lines.append(f"порожніх raw-файлів (0 рядків даних): {summary.files_empty}")
    lines.extend(summary.errors)
    lines.append(f"збагачено: {summary.lines_enriched}")
    return "\n".join(lines)


def run_enrich(config: Config, fetch: Fetcher | None = None, dry_run: bool = False) -> EnrichSummary:
    summary = EnrichSummary(dry_run=dry_run)
    fetch = fetch or fetch_deals_data
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
                    process_file(conn, config, path, summary, fetch, dry_run)
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
