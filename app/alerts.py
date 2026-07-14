"""Telegram notifications. No-op when TELEGRAM_* env is not configured."""

from __future__ import annotations

import logging

import httpx

from .config import Config

log = logging.getLogger("ubki.alerts")


def send_telegram(config: Config, text: str) -> bool:
    if not config.telegram_bot_token or not config.telegram_chat_id:
        log.info("telegram not configured, alert skipped", extra={"event": "alert_skipped"})
        return False
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    try:
        resp = httpx.post(
            url,
            json={"chat_id": config.telegram_chat_id, "text": text[:4000]},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        # Alerts must never break the upload pass.
        log.error("telegram alert failed", extra={"event": "alert_failed", "error": str(exc)})
        return False
