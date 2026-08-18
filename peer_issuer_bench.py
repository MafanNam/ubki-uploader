"""Measure the cost and the payoff of the `dwho` peer-issuer fallback, on the
host that actually runs the enricher.

Why this exists: the fallback (`fetch_passport_issuers` + `_find_peer_document`)
adds a second cabinet query per file. Measured from a laptop over a home link it
cost ~110ms per client — random-I/O bound on a ~27GB table whose lookup columns
are not covered by an index — which extrapolates to ~15 min for a full daily
file and could eat the 05:30 -> 06:00 window. That number is only trustworthy
when measured where the enricher runs, so run this there before/after deploying.

It replays the REAL production functions (`FETCH_SQL`, `_clients_missing_issuer`,
`fetch_passport_issuers`, `build_doc`) against the live cabinet DB, so it
measures the code that will actually run — not an approximation.

STRICTLY READ-ONLY: SELECT queries only, no writes to MySQL, SQLite or disk.
It does put read load on the cabinet DB, so prefer off-peak and a modest sample.
No personal data is printed — counts, timings and generic reasons only.

    docker cp peer_issuer_bench.py ubki-uploader-api-1:/tmp/peer_issuer_bench.py
    docker exec -w /src/ubki-uploader ubki-uploader-api-1 python3 /tmp/peer_issuer_bench.py
    # options: SAMPLE=2000 (applications to replay)  PROJECT=8000 (clients/day
    #          to extrapolate to)  APP_ROOT=/src/ubki-uploader
    #          OFFSET=0 (how far back from the newest id to start sampling)

Run it TWICE with different OFFSETs and compare: InnoDB's buffer pool caches
whatever was read recently, and on a ~27GB table that makes a 50x difference
(measured 110ms/client cold vs 2ms/client warm on the SAME rows). A daily file
touches the whole client base, so the cold figure is the one to budget for —
use a large OFFSET (an id window nobody has queried lately) for the honest
number, and treat a repeat run over the same window as warm-cache noise.
"""

from __future__ import annotations

import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.environ.get("APP_ROOT", "/src/ubki-uploader"))

from app.config import load_config                                      # noqa: E402
from app.enricher import (                                              # noqa: E402
    _clients_missing_issuer, _iso_date, _open_cabinet, build_doc,
    fetch_deals_data, fetch_passport_issuers,
)

SAMPLE = int(os.environ.get("SAMPLE") or 2000)
PROJECT = int(os.environ.get("PROJECT") or 8000)
OFFSET = int(os.environ.get("OFFSET") or 0)


def main() -> int:
    config = load_config()
    if not config.mysql_host:
        print("MYSQL_* env is not configured here — run this where the enricher runs",
              file=sys.stderr)
        return 2

    t0 = time.time()
    conn = _open_cabinet(config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM finplugs_creditup_applications"
                " WHERE id <= (SELECT MAX(id) - %s FROM finplugs_creditup_applications)"
                " AND id > (SELECT MAX(id) - %s FROM finplugs_creditup_applications)"
                " ORDER BY id DESC LIMIT %s",
                (OFFSET, OFFSET + SAMPLE, SAMPLE),
            )
            ids = [str(row["id"]) for row in cur.fetchall()]
    finally:
        conn.close()
    print(f"sampled application ids : {len(ids)} (offset {OFFSET})"
          f"  ({time.time() - t0:.1f}s)")

    t1 = time.time()
    rows = fetch_deals_data(config, ids)
    bulk = time.time() - t1
    print(f"fetch_deals_data        : {len(rows)} rows  ({bulk:.1f}s)"
          f"  [the query the enricher already runs today]")

    need = _clients_missing_issuer(rows)
    print(f"clients missing issuer  : {len(need)}")
    if not need:
        print("nothing to look up in this sample — try a larger SAMPLE")
        return 0

    t2 = time.time()
    peers = fetch_passport_issuers(config, need)
    peer_cost = time.time() - t2
    per_client = peer_cost / len(need)
    print(f"fetch_passport_issuers  : {len(peers)} clients with candidates"
          f"  ({peer_cost:.1f}s, {per_client * 1000:.0f}ms/client)   <-- THE NEW COST")

    # replay build_doc with and without the fallback to get the real payoff
    considered = recovered = 0
    after = Counter()
    need_set = set(need)
    for row in rows.values():
        if row["user_id"] not in need_set:
            continue
        vdate = _iso_date(row.get("applied_at"))
        if vdate is None:
            continue
        considered += 1
        old_doc, _ = build_doc(row, vdate, None)
        new_doc, new_reason = build_doc(row, vdate, peers.get(row["user_id"]))
        after["document built" if new_doc else (new_reason or "?")[:50]] += 1
        if old_doc is None and new_doc is not None:
            recovered += 1

    print()
    print(f"applications replayed   : {considered}")
    if considered:
        print(f"recovered by fallback   : {recovered} ({100 * recovered / considered:.1f}%)")
    print("outcome after the change:")
    for reason, count in after.most_common():
        print(f"    {count:6}  {reason}")

    print()
    print(f"projection for {PROJECT} clients/day: "
          f"{PROJECT * per_client / 60:.1f} min of extra enrich time")
    print("compare against the 05:30 -> 06:00 window; if it does not fit, ask for a")
    print("covering index on (user_id, passport_issued_by, passport_number,")
    print("passport_date, applied_at) or move the enrich cron earlier.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
