# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service does

Daily push of credit data to UBKI (Ukrainian credit bureau). Data files (`.txt`, JSONL inside: the line is the bare subject object **without** a `{"fo_cki": …}` wrapper) land in `UBKI_DATA_FOLDER_PATH`; each non-empty line is one subject and is sent as **one HTTP request** to UBKI. Successfully processed files move to `archive/`; per-line state lives in SQLite so nothing is ever sent twice. A read-only FastAPI facade (localhost-only, SSH tunnel in prod) exposes statuses and manual retries.

## Commands

```bash
.venv/bin/python -m pytest -q                          # full suite
.venv/bin/python -m pytest tests/test_client.py -q     # one file
.venv/bin/python -m pytest -k "test_abort" -q          # one test

python -m app.run_once --dry-run    # scan + report, zero DB writes, no sending
python -m app.run_once              # one real pass (needs env vars, see .env.example)

uvicorn "app.api:create_app" --factory --port 8000     # API (factory pattern — not app.api:app)

docker compose build
docker compose up -d                # services: api (uvicorn) + scheduler (supercronic, 06:00 Kyiv)
```

Config comes **only from env vars** (`app/config.py`); tests construct `Config` dataclasses directly instead of monkeypatching env.

## Architecture

Pipeline (`app/uploader.py: run_pass`): flock lock → scan folder → ingest new files → send records sequentially → archive completed files → write `runs` row → Telegram alert. Every record update commits its own transaction, so a crash mid-pass loses no progress.

Key invariants that span multiple files:

- **File identity = `filename + sha256`** (files can be overwritten with the same name → new identity, re-ingested). Ingestion copies every line into `records.raw_line`; from that moment the DB, not the filesystem, is the source of truth. That is why a file can be archived while its rejected lines are still retryable via API.
- **A file is archived when all its records are terminal** (`sent`/`rejected`). `failed` records keep the file in the inbox; rescans skip ingestion because the identity already exists.
- **The raw line is embedded into the UBKI envelope byte-for-byte** (`ubki_client.build_envelope` does string interpolation, never `json.loads`/`dumps` of the line). The service must not parse or validate file contents — do not "fix" this.
- **Status model** (`app/db.py`): records are `pending|sent|failed|rejected`; file status is a pure aggregate recomputed by `recompute_file_status` (a file ingested with zero records counts as `sent` and gets archived). UBKI response mapping (in `ubki_client._map_state`, reads `sentdatainfo`): `ok`/`nt` → `sent` (`nt` and `ig > 0` = accepted with warnings, counted for alerts), `er` → `rejected` (manual retry only; `last_error` carries `main_errcode` + `items[].msg`), `sy`/network/5xx → `failed` (auto-retried next pass until `attempts >= retry_cap`, default 5). Partial rejection (`er > 0` counter inside an accepted response) → `rejected`. A body carrying `state` wins over the HTTP code — UBKI pairs validation rejections with **HTTP 400** (confirmed live), and those are `rejected`, not retryable failures.
- **Session handling**: UBKI sessid is valid until 23:59:59 Kyiv time and UBKI asks to authenticate once per day. `DbSessionStore` (uploader) caches it in the `meta` table keyed by Kyiv date; `UbkiClient` re-auths once on `sentdatainfo.main_errcode`/top-level errcode 2014 or HTTP 401/403, then retries the record. Per-component `errcode`s inside `items[]` must never trigger a re-auth; "session rejected twice" is network-like (see abort) so a dead session can't cause a per-record auth storm (UBKI forbids frequent auth).
- **Abort semantics**: 3 consecutive network-like errors (transport, 5xx, `state=sy`, dead session) abort the pass (UBKI is down); remaining records stay `pending` for the next pass, and the run is recorded as `runs.status='aborted'` — **not** `success` — so the health 25h rule keeps working through an outage. `UploadResult.is_network_error` is what drives the abort — set it correctly for any new failure mode.
- **Scan filter**: only files matching `FILE_GLOB` (default `*.txt` — the producer always delivers `.txt`) are picked up; anything else in the folder is logged (`file_skipped_pattern`) and never sent.
- **Health** (`GET /health`): `degraded` when the last *successful run* (not send — days with zero files are healthy; `aborted` runs don't count) is older than 25h, or when rejected / failed-beyond-cap records exist. `GET /runs` lists pass history.

UBKI protocol details (endpoints, envelope shape, wiki links, error codes) are documented in the docstring of `app/ubki_client.py`.

## Gotchas

- JSON logging (`app/jsonlog.py`) passes `extra` keys straight into `LogRecord`: reserved attribute names (`filename`, `module`, `lineno`, …) raise `KeyError` at runtime. Use `file`, `line_no`, etc. Tests enable INFO level via an autouse fixture precisely to catch this — keep that fixture.
- `fastapi.testclient` / `httpx.MockTransport` are the only HTTP mocking tools used; there is no responses/respx dependency.
- Python 3.12 in Docker; local venv is 3.14 — don't use 3.13+-only features.
- The API is deliberately not CRUD: the DB records transmission facts. The only mutations are the token-guarded retry endpoints (`failed|rejected` → `pending`) and `POST /run`.

## Live verification status

Fully confirmed end-to-end on `test.ubki.ua` (2026-07-15, seeded sessid): **`state=ok` received**, record `sent`, file archived. Validation rejections arrive as **HTTP 400** with `sentdatainfo.state=er` in the body (handled: body `state` wins over the HTTP code). The auth endpoint enforces an IP whitelist (error 278; `upload/data` itself does not) — seed a session with `python -m app.set_session <sessid>` when authing from a non-whitelisted IP.

Data-side requirements discovered live (for the data producer): the line is the bare subject object (no `fo_cki` wrapper); all of `idents`/`docs`/`addrs`/`contacts` blocks are mandatory (2078/2077/2072/2074); contacts need a **valid phone** — made-up numbers are dropped (IGNORED 3013; on the test contour use the doc's official test numbers, e.g. `+380981220000`) and test-looking emails too (3017); deals older than the transmission window are dropped (IGNORED 3019); a new deal with a stale doc `vdate` warns (3022); the test base substitutes fake INNs and surnames (NOTICE 5009).
