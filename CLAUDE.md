# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service does

Daily push of credit data to UBKI (Ukrainian credit bureau), in two cron stages:

1. **Enricher** (`python -m app.enrich`, 05:30 Kyiv): producer drops `.txt` JSONL files (only `inn`, name, `bdate`, `deals`) into `RAW_FOLDER`; the enricher joins the cabinet MySQL (`dlref` = `finplugs_creditup_applications.id`) and writes complete fo_cki subjects (idents/docs/addrs/contacts, `person_id`=users.id, `is_gone`="0", injected `dlvidobes`) into the uploader inbox. Un-enrichable lines go to `RAW_FOLDER/quarantine/<same name>` with reasons (drop the file back into raw to reprocess); consumed raw files go to `RAW_FOLDER/processed/`.
2. **Uploader** (`python -m app.run_once`, 06:00 Kyiv): enriched files land in `UBKI_DATA_FOLDER_PATH` (`= RAW_FOLDER/enriched` in compose); each non-empty line is one subject sent as **one HTTP request** to UBKI. Successfully processed files move to `archive/`; per-line state lives in SQLite so nothing is ever sent twice.

**Invariant boundary**: the enricher legitimately parses and rebuilds lines; the uploader must NEVER parse them (the line is embedded into the envelope byte-for-byte, without a `{"fo_cki": …}` wrapper — the enricher writes the bare subject object). A read-only FastAPI facade (localhost-only, SSH tunnel in prod) exposes statuses and manual retries.

## Commands

```bash
.venv/bin/python -m pytest -q                          # full suite
.venv/bin/python -m pytest tests/test_client.py -q     # one file
.venv/bin/python -m pytest -k "test_abort" -q          # one test

python -m app.run_once --dry-run    # scan + report, zero DB writes, no sending
python -m app.run_once              # one real pass (needs env vars, see .env.example)
python -m app.enrich --dry-run      # scan raw folder + report, no MySQL, no writes
python -m app.enrich                # enrich raw files (needs RAW_FOLDER + MYSQL_* env)

uvicorn "app.api:create_app" --factory --port 8000     # API (factory pattern — not app.api:app)

docker compose build
docker compose up -d                # services: api (uvicorn) + scheduler (supercronic, 06:00 Kyiv)
```

Config comes **only from env vars** (`app/config.py`); tests construct `Config` dataclasses directly instead of monkeypatching env.

## Architecture

Pipeline (`app/uploader.py: run_pass`): flock lock → scan folder → ingest new files → send records sequentially → archive completed files → write `runs` row → Telegram alert. Every record update commits its own transaction, so a crash mid-pass loses no progress.

Enricher (`app/enricher.py: run_enrich`, own flock): scan `raw_folder` (same FILE_GLOB/mtime rules via `scan_folder(folder=...)`) → per file: parse lines, one batch MySQL query for all dlrefs (`fetch_deals_data`, injectable in tests) → `enrich_line` builds the subject from the NEWEST application's snapshot (`vdate` = `applied_at`, `users` fallback for passport/phone) → enriched file written atomically into the inbox, quarantined lines (`{"line_no","reason","line"}`) + raw file moved aside → `enriched_files` row (identity = filename+sha256, same idempotency pattern) → Telegram alert on quarantines/errors. Quarantine reasons that BLOCK a line: broken JSON, missing/unknown dlref, deals of different clients, **inn mismatch vs `users.social_number`** (never risk another person's credit history), unsupported passport format (2 letters+6 digits → dtype 1; 9 digits → dtype 17 sent WITHOUT eddr in v1), **empty document issuer `dwho`** (live-confirmed: the bureau drops such docs via IGNORED 3003 → CRITICAL 2077 rejects the package; ~20% of cabinet clients had it empty on the first mass run), no valid phone. Optional dictionary fields (csex/family/ceduc/…) are deliberately not sent in v1 — the id→UBKI-code mappings live in the OctoberCMS code, not in the DB.

Key invariants that span multiple files:

- **File identity = `filename + sha256`** (files can be overwritten with the same name → new identity, re-ingested). Ingestion copies every line into `records.raw_line`; from that moment the DB, not the filesystem, is the source of truth. That is why a file can be archived while its rejected lines are still retryable via API.
- **A file is archived when all its records are terminal** (`sent`/`rejected`). `failed` records keep the file in the inbox; rescans skip ingestion because the identity already exists.
- **The raw line is embedded into the UBKI envelope byte-for-byte** (`ubki_client.build_envelope` does string interpolation, never `json.loads`/`dumps` of the line). The service must not parse or validate file contents — do not "fix" this.
- **Status model** (`app/db.py`): records are `pending|sent|failed|rejected`; file status is a pure aggregate recomputed by `recompute_file_status` (a file ingested with zero records counts as `sent` and gets archived). UBKI response mapping (in `ubki_client._map_state`, reads `sentdatainfo`; `state` and counters are deliberately read from the same shallow dict): `ok`/`nt` → `sent` (state `nt`, counter `nt > 0` or `ig > 0` = accepted with warnings, counted for alerts — live-confirmed that `state=ok` can carry `nt` notices like the test-base INN substitution), `er` → `rejected` (manual retry only; `last_error` carries `main_errcode` + `items[].msg`), `sy`/network/5xx → `failed`. Non-network failures are auto-retried next pass until `attempts >= retry_cap` (default 5); **network-like failures don't increment `attempts`** so an outage can never exhaust a record's auto-retries. Partial rejection (`er > 0` counter inside an accepted response) → `rejected`. A body carrying `state` wins over the HTTP code **only for < 500 responses** — UBKI pairs validation rejections with **HTTP 400** (confirmed live); a 5xx is always a transport failure regardless of body (a gateway error page must not defeat the abort logic), and the 2014 session-errcode check applies to 200 bodies only.
- **Session handling**: UBKI sessid is valid until 23:59:59 Kyiv time and UBKI asks to authenticate once per day. `DbSessionStore` (uploader) caches it in the `meta` table keyed by Kyiv date; `UbkiClient` re-auths once on `sentdatainfo.main_errcode`/top-level errcode 2014 or HTTP 401/403, then retries the record. Per-component `errcode`s inside `items[]` must never trigger a re-auth; "session rejected twice" is network-like (see abort) so a dead session can't cause a per-record auth storm (UBKI forbids frequent auth).
- **Abort semantics**: 3 consecutive network-like errors (transport, 5xx, `state=sy`, dead session) abort the pass (UBKI is down); remaining records stay `pending` for the next pass, and the run is recorded as `runs.status='aborted'` — **not** `success` — so the health 25h rule keeps working through an outage. `UploadResult.is_network_error` is what drives the abort — set it correctly for any new failure mode.
- **Scan filter**: only files matching `FILE_GLOB` (default `*.txt` — the producer always delivers `.txt`) are picked up; anything else in the folder is logged (`file_skipped_pattern`), counted as `files_skipped`, and surfaced in the Telegram alert — never sent. Files ingested with zero data lines complete as `sent` but are counted as `files_empty` and alerted (a truncated export must not pass unnoticed).
- **Health** (`GET /health`): `degraded` when the last *successful run* (not send — days with zero files are healthy; `aborted` runs don't count) is older than 25h, or when rejected / failed-beyond-cap records exist. `GET /runs` lists pass history.

UBKI protocol details (endpoints, envelope shape, wiki links, error codes) are documented in the docstring of `app/ubki_client.py`.

## Gotchas

- JSON logging (`app/jsonlog.py`) passes `extra` keys straight into `LogRecord`: reserved attribute names (`filename`, `module`, `lineno`, …) raise `KeyError` at runtime. Use `file`, `line_no`, etc. Tests enable INFO level via an autouse fixture precisely to catch this — keep that fixture.
- `fastapi.testclient` / `httpx.MockTransport` are the only HTTP mocking tools used; there is no responses/respx dependency.
- Python 3.12 in Docker; local venv is 3.14 — don't use 3.13+-only features.
- **Never touch the SQLite file from the macOS host while containers are writing** (bind mount = two kernels; host-side `sqlite3` reads during an active pass silently ate committed WAL frames on a live run — 39 record updates vanished). Inspect via `docker compose exec … python/sqlite` or the API instead.
- The API is deliberately not CRUD: the DB records transmission facts. The only mutations are the token-guarded retry endpoints (`failed|rejected` → `pending`) and `POST /run`.

## Live verification status

Fully confirmed end-to-end on `test.ubki.ua` (2026-07-15, seeded sessid): **`state=ok` received**, record `sent`, file archived. Validation rejections arrive as **HTTP 400** with `sentdatainfo.state=er` in the body (handled: body `state` wins over the HTTP code). The auth endpoint enforces an IP whitelist (error 278; `upload/data` itself does not) — seed a session with `python -m app.set_session <sessid>` when authing from a non-whitelisted IP.

Data-side requirements discovered live (for the data producer): the line is the bare subject object (no `fo_cki` wrapper); all of `idents`/`docs`/`addrs`/`contacts` blocks are mandatory (2078/2077/2072/2074); contacts need a **valid phone** — made-up numbers are dropped (IGNORED 3013; on the test contour use the doc's official test numbers, e.g. `+380981220000`) and test-looking emails too (3017); deals older than the transmission window are dropped (IGNORED 3019); a new deal with a stale doc `vdate` warns (3022); the test base substitutes fake INNs and surnames (NOTICE 5009).
