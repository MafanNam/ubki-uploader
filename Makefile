.PHONY: help test test-file test-k enrich enrich-dry run run-dry api build up down logs

PYTHON := .venv/bin/python

help:
	@echo "test              - run full test suite"
	@echo "test-file f=path  - run one test file (make test-file f=tests/test_client.py)"
	@echo "test-k k=expr     - run tests matching -k expr (make test-k k=test_abort)"
	@echo "enrich-dry        - enrich dry run (scan raw folder, no MySQL, no writes)"
	@echo "enrich            - run enricher"
	@echo "run-dry           - uploader dry run (scan + report, zero DB writes)"
	@echo "run               - run uploader once"
	@echo "api               - start API with uvicorn (factory pattern)"
	@echo "build             - docker compose build"
	@echo "up                - docker compose up -d"
	@echo "down              - docker compose down"
	@echo "logs              - docker compose logs -f"

test:
	$(PYTHON) -m pytest -q

test-file:
	$(PYTHON) -m pytest $(f) -q

test-k:
	$(PYTHON) -m pytest -k "$(k)" -q

enrich-dry:
	$(PYTHON) -m app.enrich --dry-run

enrich:
	$(PYTHON) -m app.enrich

run-dry:
	$(PYTHON) -m app.run_once --dry-run

run:
	$(PYTHON) -m app.run_once

api:
	.venv/bin/uvicorn "app.api:create_app" --factory --port 8000

build:
	docker compose build

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f
