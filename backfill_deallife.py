#!/usr/bin/env python3
"""Backfill generator for UBKI CRITICAL 2091: expand each stuck deal's single
current `deallife` slice into a FULL monthly chain from the overdue start
(`dldpf`) to the deal's own current period, so `dldayexp` grows smoothly (steps
of 28-31 days) instead of jumping by hundreds of days at once.

Why: UBKI stores the last period it received for a deal and refuses an update
whose `dldayexp` jumped more than the allowed step (the cap is not a fixed 92 —
it scales with the gap between periods). For ~9.9k deals the bureau's last
stored period is 2026-05 (the OLD service's last month, which reported
`dldayexp=0`), so every current package is rejected outright. The fix that
already worked once (2026-07-24…30, 61638 of 63977 accepted) is to re-send the
whole history as one package per subject.

    !!! Requires UBKI to open access first (see LETTER_UBKI_2091.md):
        - the transmission WINDOW back to the oldest `dldpf` (2016-01 in the
          current set; measured, not guessed), else
          old slices come back IGNORED 3019 and the jump stays;
        - permission to OVERWRITE stored periods through 2026-05.

This is NOT part of the daily pipeline. The uploader still treats the produced
line as opaque bytes (`build_envelope` embeds it byte-for-byte); only this
generator parses subjects, exactly as the enricher legitimately does.

Default source is the uploader DB: the subjects it actually rejected with 2091
(`records.raw_line` is the enriched line, so no re-enrichment is needed and the
scope is precise). `--input` reads an enriched file instead.

Usage (inside the container on the prod host):
    python3 backfill_deallife.py --sample
    python3 backfill_deallife.py --limit 3 --output /ubki-data/enriched/VigruzkaUBKI_2091_CANARY.txt
    python3 backfill_deallife.py --output /ubki-data/enriched/VigruzkaUBKI_2091_BACKFILL.txt
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from calendar import monthrange
from datetime import date
from pathlib import Path

DEFAULT_DB = "/data/ubki.sqlite3"
MAX_STEP_DAYS = 92          # a bigger jump than this is what 2091 complains about
MAX_LINE_BYTES = 1_800_000  # stay under UBKI's 2 MB/request (errcode 2039)
# dictionary 16: statuses that assert the deal is over. Their template carries
# zeroed amounts and a closing date, so replicating it across historical periods
# would claim the deal was already closed back then — never expand those.
FINAL_STATUSES = {2, 3, 6, 7, 10, 11, 12, 13, 14}
# match the enricher's on-disk style: compact JSON, raw UTF-8 Cyrillic
_DUMP = dict(ensure_ascii=False, separators=(",", ":"))


# --- input -------------------------------------------------------------------

def read_file_lines(path: Path) -> list[str]:
    """Non-blank lines, UTF-8 with a cp1251 fallback (same pattern as
    app.enricher._read_numbered_lines: the producer occasionally exports
    Cyrillic in the Windows codepage)."""
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("cp1251")
    return [line for line in text.splitlines() if line.strip()]


def read_db_lines(db_path: str, file_id: int | None, errcode: str) -> tuple[list[str], int]:
    """Enriched lines of the records UBKI rejected with `errcode`. Read-only, and
    only from inside the container — never touch the SQLite file from the host
    while the containers may be writing."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    if file_id is None:
        row = conn.execute(
            "SELECT MAX(file_id) FROM records WHERE status='rejected' AND last_error LIKE ?",
            (f"%{errcode}%",)).fetchone()
        file_id = row[0]
        if file_id is None:
            return [], -1
    rows = conn.execute(
        "SELECT raw_line FROM records WHERE file_id=? AND status='rejected' AND last_error LIKE ?",
        (file_id, f"%{errcode}%")).fetchall()
    return [r[0] for r in rows], file_id


# --- expansion ---------------------------------------------------------------

def parse_iso(value) -> date | None:
    try:
        y, m, d = map(int, str(value).split("-"))
        return date(y, m, d)
    except Exception:
        return None


def month_iter(start: date, end_year: int, end_month: int):
    """(year, month) from start's month up to (end_year, end_month) EXCLUSIVE —
    the final period is the deal's own slice, appended separately."""
    y, m = start.year, start.month
    while (y, m) < (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1


def _clean(template: dict, stats: dict) -> dict:
    """Fresh copy; drop a malformed `dldff` (an unparseable date crashes UBKI
    with a NullPointerException surfaced as CRITICAL 2001)."""
    s = dict(template)
    if "dldff" in s and parse_iso(s.get("dldff")) is None:
        s.pop("dldff")
        stats["dldff_stripped"] += 1
    return s


def _make_slice(template: dict, dpf: date, clc: date, stats: dict,
                historical: bool) -> dict:
    """One monthly slice built from the CURRENT state, with `dldayexp` DERIVED
    from the calculation date, so the chain is monotonic by construction (dates
    rise → days rise). Deriving it also repairs the ~0.4% of producer rows whose
    own `dldayexp` contradicts their dates — kept verbatim they would break
    monotonicity and get the whole package rejected.

    Known simplification: amounts and flags are the current values, because the
    cabinet keeps no historical balances. Only the overdue-day count, which is a
    pure function of the dates, is reconstructed truthfully.

    A historical slice never carries `dldff`: an end date outside the slice's own
    period is exactly what UBKI answers with 2056."""
    s = _clean(template, stats)
    s["dlmonth"] = clc.month
    s["dlyear"] = clc.year
    s["dldateclc"] = clc.isoformat()
    s["dldayexp"] = max(0, (clc - dpf).days)
    if historical and s.pop("dldff", None) is not None:
        stats["dldff_dropped_historical"] += 1
    return s


def expand_deal(deal: dict, stats: dict, max_step: int, include_final: bool) -> dict:
    life = deal.get("deallife") or []
    if len(life) != 1:
        stats["deal_unexpected_shape"] += 1
        return deal  # leave anything we don't recognise untouched

    current = life[0]
    try:
        exp = int(current.get("dldayexp") or 0)
    except (TypeError, ValueError):
        exp = 0
    dpf = parse_iso(current.get("dldpf"))
    try:
        flstat = int(current.get("dlflstat"))
    except (TypeError, ValueError):
        flstat = None

    try:
        amt_exp = float(current.get("dlamtexp") or 0)
    except (TypeError, ValueError):
        amt_exp = 0.0

    keep = None
    if exp <= max_step:
        keep = "below_threshold"      # this deal cannot be tripping 2091
    elif dpf is None:
        keep = "bad_dldpf"            # nothing to derive the chain from
    elif flstat in FINAL_STATUSES and not include_final:
        keep = "final_status"         # closing a deal is a separate operation
    elif amt_exp == 0:
        # every generated slice would carry overdue days next to a zero overdue
        # amount -> CRITICAL 2051 on the whole package. We cannot invent the
        # historical amount, so leave the deal alone and report it (none in the
        # 2026-09 set, but a silent mass rejection later is not acceptable)
        keep = "amtexp_zero"
    if keep:
        stats["kept_" + keep] += 1
        out = dict(deal)
        out["deallife"] = [_clean(current, stats)]
        return out

    # the final period keeps the deal's OWN month and calculation date; only its
    # dldayexp is recomputed, for the monotonicity reason above
    end_year = int(current.get("dlyear") or 0)
    end_month = int(current.get("dlmonth") or 0)
    final_clc = parse_iso(current.get("dldateclc"))
    if not (1 <= end_month <= 12 and end_year >= 2000):
        stats["kept_bad_period"] += 1
        out = dict(deal)
        out["deallife"] = [_clean(current, stats)]
        return out
    if final_clc is None:
        final_clc = date(end_year, end_month, monthrange(end_year, end_month)[1])

    # A slice whose derived dldayexp is 0 is DROPPED, not sent: `dldpf` often falls
    # on the last day of its month (21.9% of the stuck set), and then the first
    # slice would carry 0 overdue days next to a non-zero `dlamtexp` — which the
    # bureau rejects outright with CRITICAL 2051 ("dldayexp cannot be 0 if
    # dlamtexp is not 0, and vice versa"). Live-confirmed on the prod canary
    # 2026-09-08. Only the first slice can be affected (later dates are strictly
    # further from dldpf), so the chain simply starts one month later.
    chain = []
    for y, m in month_iter(dpf, end_year, end_month):
        slice_ = _make_slice(current, dpf, date(y, m, monthrange(y, m)[1]), stats, True)
        if slice_["dldayexp"] == 0:
            stats["zero_day_slices_dropped"] += 1
            continue
        chain.append(slice_)
    chain.append(_make_slice(current, dpf, final_clc, stats, False))

    out = dict(deal)
    out["deallife"] = chain
    stats["deals_expanded"] += 1
    stats["slices_written"] += len(chain)
    stats["max_slices"] = max(stats["max_slices"], len(chain))
    return out


def expand_subject(subject: dict, stats: dict, max_step: int, include_final: bool,
                   include_multi_deal: bool = False) -> dict:
    """A package is accepted or rejected as a WHOLE, so a subject carrying more
    than one deal is left untouched by default. Measured on prod 2026-09-08:
    of 36 multi-deal subjects sent with chains, **34 came back CRITICAL 2090**
    ("оновлення угод в кінцевому статусі не допускається") pointing at an OLD
    period (2017-2021) — the bureau holds a final status back there while the
    current period still compares (which is why the daily send shows 2091, not
    2090). In 21 of 29 cases that is the very deal 2091 complains about, so no
    chain can be written into it at all. Every one of the 30 single-deal
    subjects tested was accepted. Those 36 subjects (0.4% of the set) need a
    producer/business answer first: is the deal closed, as the bureau thinks,
    or open, as we keep exporting it?"""
    subject = dict(subject)
    deals = subject.get("deals") or []
    if len(deals) > 1 and not include_multi_deal:
        stats["kept_multi_deal"] += len(deals)
        subject["deals"] = [dict(d, deallife=[_clean((d.get("deallife") or [{}])[0], stats)])
                            for d in deals]
        return subject
    subject["deals"] = [expand_deal(d, stats, max_step, include_final) for d in deals]
    return subject


def assert_chain_sane(subject: dict, max_step: int) -> None:
    """Self-check on every expanded deal: periods strictly increasing,
    `dldayexp` non-decreasing with steps within the cap, and the first slice
    starting near zero (UBKI asks overdue to "grow from the beginning")."""
    for deal in subject.get("deals") or []:
        life = deal.get("deallife") or []
        if len(life) < 2:
            continue
        assert int(life[0]["dldayexp"]) <= 31, f"chain starts at {life[0]['dldayexp']}"
        prev_exp, prev_period = None, None
        for s in life:
            exp, period = int(s["dldayexp"]), (int(s["dlyear"]), int(s["dlmonth"]))
            # CRITICAL 2051: overdue days and overdue amount must be zero together
            amt = float(s.get("dlamtexp") or 0)
            assert (exp == 0) == (amt == 0), f"2051: dldayexp={exp}, dlamtexp={amt}"
            if prev_exp is not None:
                assert exp >= prev_exp, f"dldayexp decreased: {prev_exp} -> {exp}"
                assert exp - prev_exp <= max_step, f"step > {max_step}: {prev_exp} -> {exp}"
                assert period > prev_period, f"period not increasing: {prev_period} -> {period}"
            prev_exp, prev_period = exp, period


def _stratified(lines: list[str], want: int) -> list[str]:
    """A sample that deliberately over-represents the risky shapes rather than
    the common one: multi-deal subjects first (one bad deal rejects the whole
    package), then the longest chain and the biggest line (the 2 MB limit and
    the deepest window live there), then an even spread across the rest so the
    ordinary case is still represented."""
    parsed: list[tuple[int, int, int, str]] = []   # deals, max slices-ish, bytes, line
    for line in lines:
        try:
            subject = json.loads(line)
        except json.JSONDecodeError:
            continue
        deals = subject.get("deals") or []
        span = 0
        for deal in deals:
            current = (deal.get("deallife") or [{}])[0]
            dpf = parse_iso(current.get("dldpf"))
            if dpf and current.get("dlyear"):
                span = max(span, (int(current["dlyear"]) - dpf.year) * 12
                           + int(current.get("dlmonth") or 1) - dpf.month)
        parsed.append((len(deals), span, len(line.encode("utf-8")), line))

    picked: list[str] = []
    seen: set[str] = set()

    def take(line: str) -> None:
        if line not in seen:
            seen.add(line)
            picked.append(line)

    for entry in sorted(parsed, key=lambda e: -e[0]):
        if entry[0] < 2:
            break
        take(entry[3])
    take(max(parsed, key=lambda e: e[1])[3])       # longest chain
    take(max(parsed, key=lambda e: e[2])[3])       # biggest line
    rest = [e[3] for e in parsed if e[3] not in seen]
    if len(picked) < want and rest:
        step = max(1, len(rest) // max(1, want - len(picked)))
        for line in rest[::step]:
            if len(picked) >= want:
                break
            take(line)
    return picked


def new_stats() -> dict:
    return {"subjects": 0, "deals": 0, "deals_expanded": 0, "slices_written": 0,
            "deal_unexpected_shape": 0, "kept_below_threshold": 0, "kept_bad_dldpf": 0,
            "kept_final_status": 0, "kept_bad_period": 0, "kept_amtexp_zero": 0,
            "kept_multi_deal": 0,
            "zero_day_slices_dropped": 0, "dldff_stripped": 0,
            "dldff_dropped_historical": 0, "oversized": 0, "max_slices": 0, "max_bytes": 0}


# --- cli ---------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Expand deallife into a full monthly chain to clear UBKI 2091",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--db", default=DEFAULT_DB, help="uploader DB to take rejected lines from")
    src.add_argument("--input", help="read an enriched .txt instead of the DB")
    ap.add_argument("--file-id", type=int, default=None,
                    help="files.id to take rejections from (default: the newest one that has them)")
    ap.add_argument("--errcode", default="2091", help="rejection code to select (default: %(default)s)")
    ap.add_argument("--output")
    ap.add_argument("--limit", type=int, default=None, help="process only the first N subjects")
    ap.add_argument("--shuffle", type=int, default=None, metavar="SEED",
                    help="shuffle the source lines with this seed before --limit, so a --limit N run"
                         " is a RANDOM sample of the set instead of its first N rows")
    ap.add_argument("--probe", type=int, default=None, metavar="N",
                    help="write a STRATIFIED sample of N subjects instead of everything: every"
                         " multi-deal subject (a package is rejected as a whole, so those carry the"
                         " most risk), the longest chain, the largest line, then an even spread of"
                         " the rest. Use it to clear the shapes a --limit canary never reaches")
    ap.add_argument("--max-step", type=int, default=MAX_STEP_DAYS,
                    help="expand a deal whose dldayexp exceeds this (default: %(default)s)")
    ap.add_argument("--include-final", action="store_true",
                    help="also expand deals in a final dlflstat (NOT recommended)")
    ap.add_argument("--include-multi-deal", action="store_true",
                    help="also expand subjects carrying several deals — 34 of 36 such packages were"
                         " rejected with 2090 on prod, so this is off by default")
    ap.add_argument("--sample", action="store_true", help="print one expanded subject and exit")
    args = ap.parse_args(argv)

    if args.input:
        path = Path(args.input)
        if not path.is_file():
            print(f"input not found: {path}", file=sys.stderr)
            return 2
        lines, source = read_file_lines(path), str(path)
    else:
        lines, file_id = read_db_lines(args.db, args.file_id, args.errcode)
        source = f"{args.db} file_id={file_id} errcode={args.errcode}"
    if not lines:
        print(f"no lines to process ({source})", file=sys.stderr)
        return 1
    if args.shuffle is not None:
        random.Random(args.shuffle).shuffle(lines)
        source += f" shuffled(seed={args.shuffle})"
    print(f"source: {source} | lines: {len(lines)}", file=sys.stderr)

    stats = new_stats()

    if args.probe:
        lines = _stratified(lines, args.probe)
        print(f"стратифікована вибірка: {len(lines)}", file=sys.stderr)

    if args.sample:
        for line in lines:
            try:
                subject = json.loads(line)
            except json.JSONDecodeError:
                continue
            out = expand_subject(subject, stats, args.max_step, args.include_final, args.include_multi_deal)
            if not stats["deals_expanded"]:
                continue
            assert_chain_sane(out, args.max_step)
            deal = next(d for d in out["deals"] if len(d.get("deallife") or []) > 1)
            life = deal["deallife"]
            steps = [int(life[i]["dldayexp"]) - int(life[i - 1]["dldayexp"])
                     for i in range(1, len(life))]
            print(f"inn={out.get('inn')} dlref={deal.get('dlref')} slices={len(life)}")
            print("first :", json.dumps(life[0], **_DUMP))
            print("second:", json.dumps(life[1], **_DUMP))
            print("last  :", json.dumps(life[-1], **_DUMP))
            print(f"dldayexp {life[0]['dldayexp']}..{life[-1]['dldayexp']}"
                  f" | max step {max(steps)} (cap {args.max_step})")
            print("line bytes:", len(json.dumps(out, **_DUMP).encode("utf-8")))
            return 0
        print("no expandable subject found", file=sys.stderr)
        return 1

    if not args.output:
        print("--output is required (or use --sample)", file=sys.stderr)
        return 2

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with out_path.open("w", encoding="utf-8") as fh:
        for line in lines:
            if args.limit is not None and stats["subjects"] >= args.limit:
                break
            try:
                subject = json.loads(line)
            except json.JSONDecodeError:
                # a line that will not parse cannot be expanded; pass it through
                fh.write(line + "\n")
                written += 1
                stats["subjects"] += 1
                continue
            stats["subjects"] += 1
            stats["deals"] += len(subject.get("deals") or [])
            out = expand_subject(subject, stats, args.max_step, args.include_final, args.include_multi_deal)
            assert_chain_sane(out, args.max_step)
            payload = json.dumps(out, **_DUMP)
            nbytes = len(payload.encode("utf-8"))
            if nbytes > MAX_LINE_BYTES:
                # would hit UBKI's 2 MB limit; send the original line instead
                stats["oversized"] += 1
                fh.write(line + "\n")
                written += 1
                continue
            stats["max_bytes"] = max(stats["max_bytes"], nbytes)
            fh.write(payload + "\n")
            written += 1

    print(json.dumps({"output": str(out_path), "lines_written": written, **stats},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
