# ubki-uploader

Щоденна передача кредитних даних до УБКІ (Українське бюро кредитних історій).

JSONL-файли з'являються у папці `UBKI_DATA_FOLDER_PATH`; кожен непорожній рядок — один суб'єкт кредитної історії, який відправляється **одним HTTP-запитом** на `upload/data`. Стан кожного рядка живе в SQLite, тому ніщо не відправляється двічі. Повністю оброблені файли переїжджають в `archive/`. Read-only FastAPI-фасад (тільки localhost, у проді — через SSH-тунель) показує статуси і дозволяє ручні ретраї.

## Як це працює

```
06:00 Kyiv (supercronic)                  ┌──────────────┐
python -m app.run_once ──► flock ──► scan │ *.jsonl > 5хв│──► ingest (нові filename+sha256)
                                          └──────────────┘         │ рядки → records(pending)
                                                                   ▼
Telegram-алерт ◄── runs row ◄── archive/ ◄── усі records термінальні ◄── послідовна відправка
(тільки якщо є проблеми)                        (sent/rejected)          1 рядок = 1 запит
```

Статуси record: `pending → sent | failed | rejected`

| Відповідь УБКІ | Статус | Що далі |
|---|---|---|
| `state=ok` / `nt` | `sent` | готово; `nt` та `ig>0` рахуються як «з зауваженнями» (потрапляють в алерт) |
| `er>0` всередині прийнятої відповіді | `rejected` | ручний розбір |
| `state=er` | `rejected` | ручний розбір + ручний ретрай; причина в `last_error` (`main_errcode` + `items[].msg`) |
| `state=sy`, HTTP 5xx, мережа | `failed` | авторетрай наступним проходом, до `RETRY_CAP` (5) спроб |

3 поспіль мережеві помилки → прохід переривається (`runs.status='aborted'`), решта рядків лишаються `pending` до наступного проходу. Сесія УБКІ (`sessid`) кешується в БД до кінця доби за Києвом; повторний auth — лише при відхиленні сесії (401/403 або `main_errcode=2014`).

## Деплой

```bash
cp .env.example .env        # заповнити UBKI_LOGIN/UBKI_PASSWORD/API_TOKEN, шляхи
docker compose build
docker compose up -d        # api (127.0.0.1:8000) + scheduler (крон 06:00 Kyiv)
```

Обидва сервіси ділять `./data` (SQLite + flock-лок) і папку вхідних файлів (`${UBKI_DATA_FOLDER_PATH}` → `/ubki-data` всередині контейнера).

### Змінні оточення

| Змінна | Обов'язкова | Дефолт | Опис |
|---|---|---|---|
| `UBKI_DATA_FOLDER_PATH` | так | — | на хості: шлях для volume; всередині контейнера завжди `/ubki-data` |
| `UBKI_LOGIN`, `UBKI_PASSWORD` | так | — | креденшили УБКІ |
| `API_TOKEN` | так | — | статичний токен для POST-ендпоінтів (заголовок `X-API-Token`) |
| `UBKI_URL` | ні | `https://secure.ubki.ua/upload/data` | тест-контур: `https://test.ubki.ua/upload/data` |
| `UBKI_AUTH_URL` | ні | `https://secure.ubki.ua/b2_api_xml/ubki/auth` | тест: `https://test.ubki.ua/b2_api_xml/ubki/auth` |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | ні | — | алерти; без них — no-op |
| `FILE_GLOB` | ні | `*.jsonl` | які файли брати з папки; решта логується і не чіпається |
| `DB_PATH` | ні | `/data/ubki.sqlite3` | ставиться compose'ом |
| `RETRY_CAP` | ні | `5` | межа авторетраїв для `failed` |
| `MIN_FILE_AGE_SEC` | ні | `300` | захист від недописаних файлів |

Не винесено в env (константи в `app/config.py`): ліміт запиту 2 MiB, поріг аборту 3, HTTP-таймаут 60с.

## Щоденна рутина оператора

Тиша в Telegram = все добре (алерт приходить тільки при failed/rejected/зауваженнях/аборті). Перевірити руками:

```bash
ssh -L 8000:127.0.0.1:8000 <prod-host>       # тунель до API

curl -s localhost:8000/health | jq            # ok | degraded + причини
curl -s "localhost:8000/runs?limit=5" | jq    # історія проходів: статус, лічильники, помилка
```

`/health` каже `degraded`, якщо: останній успішний прохід старший за 25 год (дні без файлів — теж успіх; aborted не рахується), або є `rejected`, або `failed` поза лімітом ретраїв, або недоступна папка.

## Розбір rejected

```bash
curl -s "localhost:8000/files?status=rejected" | jq          # які файли
curl -s "localhost:8000/files/<id>" | jq '.records[] | select(.status=="rejected") | {line_no, last_error}'
```

`last_error` містить `main_errcode` і повідомлення з `items[]`. Далі:

1. Помилка в даних → виправити у джерелі; виправлений файл прийде новим (нова ідентичність `filename+sha256`) і піде звичайним шляхом. Старий record можна лишити rejected (це факт передачі).
2. Помилка була тимчасова/на боці УБКІ → ручний ретрай (рядки беруться з БД, файл може бути вже в архіві):

```bash
curl -X POST -H "X-API-Token: $API_TOKEN" localhost:8000/files/<id>/retry      # весь файл
curl -X POST -H "X-API-Token: $API_TOKEN" localhost:8000/records/<id>/retry    # один рядок
curl -X POST -H "X-API-Token: $API_TOKEN" localhost:8000/run                   # не чекати 06:00
```

`POST /run` повертає 202 одразу (фоновий процес, flock не дасть перетнутись із кроном); результат дивитись у `/runs`.

## Корисні команди

```bash
docker compose logs -f scheduler                                  # JSON-логи проходів
docker compose exec scheduler python -m app.run_once --dry-run    # скан без запису/відправки
docker compose exec scheduler python -m app.set_session <sessid>  # підкласти sessid вручну
.venv/bin/python -m pytest -q                                     # тести (локально)
```

`set_session` потрібен, коли auth неможливий з поточної IP (whitelist УБКІ, помилка 278), а валідний `sessid` отримано з дозволеної адреси. Діє до 23:59:59 Києва.

## Бекап

Уся гарантія «нічого не відправимо двічі» живе в `./data/ubki.sqlite3`. Робити копію (безпечно на живій базі завдяки WAL):

```bash
sqlite3 data/ubki.sqlite3 ".backup data/backup/ubki-$(date +%F).sqlite3"
```

Розумно повісити в крон хоста + прибирати старі копії. Втрата БД = ризик повторної відправки вже переданих рядків.

## Тест-контур

Логіни для `test.ubki.ua` видає адміністратор організації: кабінет партнера → «Користувачі → Налаштування користувачів → Тестове середовище». В `.env` поміняти `UBKI_URL`/`UBKI_AUTH_URL` на тестові.

## Відомі нюанси

- **errcode 2092 («глобальний дублікат»)** при ретраї після таймауту може означати «вже доставлено попередньою спробою» — перевірити `msg` перед повторними діями.
- Якщо контейнер `scheduler` лежить — проходів немає і алертів теж немає (алерт шле сам прохід). Зовнішній моніторинг: смикати `/health` через тунель або стежити за щоденним записом у `/runs`.
- Протокол УБКІ (endpoints, envelope, коди) — у docstring `app/ubki_client.py`; посилання на wiki УБКІ там же.
