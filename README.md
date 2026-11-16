# Асинхронный сервис процессинга платежей

Микросервис асинхронной обработки платежей: принимает запрос на оплату, гарантированно
публикует событие через outbox, обрабатывает его эмуляцией платёжного шлюза (2–5 с,
90 % успех / 10 % отказ) и уведомляет клиента по webhook.

## Стек

FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL · RabbitMQ (FastStream) ·
Alembic · Docker + docker-compose · uv

## Архитектура

```
POST /api/v1/payments ──► ┌── одна транзакция ──┐ ──► 202 Accepted
(X-API-Key,               │ payments + outbox   │
 Idempotency-Key)         └─────────────────────┘
                                    │ SELECT … FOR UPDATE SKIP LOCKED
                             ┌──────▼───────┐
                             │ outbox-relay │ publish (persistent) → mark published
                             └──────┬───────┘
                                    ▼
                    exchange «payments» ──rk payments.new──► [payments.new]
                                                                  │
                                                    ┌─────────────▼─────────────┐
                                                    │        consumer           │
                                                    │ шлюз → статус → webhook   │
                                                    └─────────────┬─────────────┘
                       технический сбой │                         │ ack
                                        ▼                         │
             exchange «payments.retry» ──► [payments.retry] TTL 2s/4s
                                             DLX ──► «payments» ──┘
                       после 3-й попытки → reject → «payments.dlx» ──► [payments.dlq]
```

| Процесс | Команда | Роль |
|---|---|---|
| `api` | `uvicorn app.main:app` | HTTP; работает только с БД и не зависит от RabbitMQ |
| `outbox` | `python -m app.outbox_worker` | гарантированная публикация событий |
| `consumer` | `faststream run app.consumer:app` | один обработчик: шлюз + статус + webhook |
| `migrate` | `alembic upgrade head` | one-shot миграции |

## Быстрый старт

### Через Docker

```bash
cp .env.example .env
docker compose --profile demo up -d --build
docker compose logs -f api outbox consumer webhook
```

- Swagger: <http://localhost:8000/docs> (кнопка Authorize → `X-API-Key`)
- RabbitMQ UI: <http://localhost:15672> (`app` / `app`)
- Демо-приёмник webhook (профиль `demo`): `http://webhook:9000` — пути `/hook`, `/flaky`, `/fail`

Остановить и снести данные:

```bash
docker compose down -v
```

### Локальная разработка

```bash
uv sync
docker compose up -d db rabbitmq
uv run alembic upgrade head

uv run uvicorn app.main:app --reload      # терминал 1 — API
uv run python -m app.outbox_worker        # терминал 2 — релей
uv run faststream run app.consumer:app    # терминал 3 — консьюмер
```

Проверка:

```bash
curl -s http://localhost:8000/health/live
curl -s http://localhost:8000/health/ready
```

## Конфигурация

Секции настроек — независимые `BaseSettings` со своими префиксами, поэтому каждая читает
только свои переменные. Полный список — в `.env.example`.

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `API_KEY` | `super-secret-key` | статический ключ для всех эндпоинтов |
| `POSTGRES_HOST/PORT/USER/PASSWORD/DB` | `localhost/5432/app/app/payments` | подключение к БД |
| `POSTGRES_POOL_SIZE/MAX_OVERFLOW` | `10 / 20` | размер пула соединений |
| `RABBITMQ_HOST/PORT/USER/PASSWORD/VHOST` | `localhost/5672/app/app//` | подключение к брокеру |
| `OUTBOX_POLL_INTERVAL/BATCH_SIZE` | `1.0 / 50` | частота опроса и размер пачки релея |
| `OUTBOX_BASE_DELAY/MAX_DELAY` | `1.0 / 60.0` | backoff при недоступности брокера |
| `RETRY_MAX_ATTEMPTS/BASE_DELAY` | `3 / 2.0` | попытки обработки сообщения, задержки 2s→4s |
| `GATEWAY_MIN_DELAY/MAX_DELAY/FAILURE_RATE` | `2.0 / 5.0 / 0.1` | параметры эмуляции шлюза |
| `WEBHOOK_MAX_ATTEMPTS/BASE_DELAY/TIMEOUT/SECRET` | `3 / 1.0 / 5.0 / …` | доставка webhook и HMAC-подпись |

Контейнеру миграций достаточно `POSTGRES_*` — секции самодостаточны, лишние переменные
не требуются.

## API

Все эндпоинты требуют заголовок `X-API-Key`. Создание платежа — обязательный `Idempotency-Key`.

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-key" \
  -H "Idempotency-Key: order-1" \
  -H "Content-Type: application/json" \
  -d '{"amount":"199.90","currency":"RUB","description":"Заказ №1","metadata":{"order_id":1},"webhook_url":"http://webhook:9000/hook"}'
```

```
HTTP/1.1 202 Accepted

{"payment_id":"6f1c…","status":"pending","created_at":"2026-11-16T12:00:00Z"}
```

PowerShell-эквивалент:

```powershell
$body = @{
    amount      = "199.90"
    currency    = "RUB"
    description = "Заказ №1"
    metadata    = @{ order_id = 1 }
    webhook_url = "http://webhook:9000/hook"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/payments `
  -Headers @{ "X-API-Key" = "super-secret-key"; "Idempotency-Key" = "order-1" } `
  -ContentType "application/json" -Body $body
```

### Повтор с тем же ключом

Тот же `Idempotency-Key` возвращает **исходный** платёж: `202`, тот же `payment_id` и
заголовок `X-Idempotent-Replay: true`. Второй платёж и второе событие в outbox не создаются.

### Получение платежа

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: super-secret-key" | jq
```

```json
{
  "id": "6f1c…",
  "amount": "199.90",
  "currency": "RUB",
  "description": "Заказ №1",
  "metadata": {"order_id": 1},
  "status": "succeeded",
  "idempotency_key": "order-1",
  "webhook_url": "http://webhook:9000/hook",
  "created_at": "2026-11-16T12:00:00Z",
  "processed_at": "2026-11-16T12:00:04Z",
  "error": null,
  "webhook_status": "delivered",
  "webhook_attempts": 1
}
```

### Коды ответов

| Ситуация | Ответ |
|---|---|
| Новый платёж | `202` + `payment_id`, `status`, `created_at` |
| Повтор с тем же `Idempotency-Key` | `202`, тот же `payment_id`, `X-Idempotent-Replay: true` |
| Нет или неверный `X-API-Key` | `401` |
| Нет `Idempotency-Key` / невалидное тело | `422` |
| Платёж не найден | `404` |

`amount` — строка (`NUMERIC(18,2)`). Валюты (`RUB`, `USD`, `EUR`) и статусы
(`pending`, `succeeded`, `failed`) — native PostgreSQL ENUM.

## Webhook

После завершения обработки (успех или отказ шлюза) консьюмер отправляет уведомление
на `webhook_url`, указанный при создании платежа.

### Формат

`POST` на `webhook_url` с телом:

```json
{"payment_id":"6f1c…","status":"succeeded","amount":"199.90","currency":"RUB",
 "idempotency_key":"order-1","processed_at":"2026-11-16T12:00:04Z"}
```

Поля: `payment_id`, `status` (`succeeded` или `failed`), `amount`, `currency`,
`idempotency_key`, `processed_at`.

### Заголовки и подпись

| Заголовок | Значение |
|---|---|
| `Content-Type` | `application/json` |
| `X-Payment-Id` | ID платежа |
| `X-Signature` | `sha256=<HMAC-SHA256(WEBHOOK_SECRET, raw_body)>` |

Проверка подписи на стороне приёмника:

```python
import hashlib
import hmac

def verify_signature(secret: str, raw_body: bytes, signature_header: str) -> bool:
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    received = signature_header.removeprefix("sha256=")
    return hmac.compare_digest(expected, received)
```

### Повторные попытки и статус доставки

- Успех — любой ответ `2xx`.
- При ошибке сети или статусе вне `2xx` — до 3 повторных попыток с экспоненциальной
  задержкой 1s → 2s (`WEBHOOK_MAX_ATTEMPTS`, `WEBHOOK_BASE_DELAY`).
- Итог доставки фиксируется в полях платежа `webhook_status` (`pending`, `delivered`, `failed`)
  и `webhook_attempts`. Неудачная доставка **не откатывает** статус платежа.
- Доставка — at-least-once: приёмник должен быть идемпотентен по `payment_id`.

## Гарантии доставки

### Outbox

`POST /api/v1/payments` пишет платёж в `payments` и событие в `outbox` **в одной транзакции**.
Отдельный процесс `outbox` выбирает необработанные события `FOR UPDATE SKIP LOCKED`,
публикует persistent-сообщение и помечает `published`.

- At-least-once: если процесс упадёт между publish и commit, событие уйдёт повторно.
  Consumer идемпотентен — повторная обработка не создаст второй платёж.
- API **не подключается к RabbitMQ** и работает, даже если брокер недоступен. События
  накапливаются в `outbox` и разъезжаются после восстановления.

### Идемпотентность

- **HTTP** — `UNIQUE(idempotency_key)` в БД. Повторный запрос с тем же ключом возвращает
  исходный платёж (`202`, тот же `payment_id`, заголовок `X-Idempotent-Replay: true`).
  Гонка параллельных запросов разруливается через `IntegrityError`.
- **Consumer** — обработка только при `status = pending`. Повторная доставка сообщения
  не списывает деньги дважды и не отправляет webhook повторно.

### Топология RabbitMQ

| Объект | Тип | Назначение |
|---|---|---|
| `payments` | exchange (direct) | основной маршрут, rk `payments.new` |
| `payments.new` | queue | обработка; DLX → `payments.dlx` |
| `payments.retry` | exchange + queue | отсрочка per-message TTL, DLX → `payments` |
| `payments.dlx` / `payments.dlq` | exchange + queue | окончательно упавшие сообщения |

### Retry и DLQ

- **Технические сбои** (БД, шлюз недоступен, сеть) — до 3 попыток с экспоненциальной
  задержкой 2s → 4s (`RETRY_MAX_ATTEMPTS`, `RETRY_BASE_DELAY`). После исчерпания — reject
  без requeue, сообщение уходит через DLX в `payments.dlq`.
- **Отказ шлюза** (те самые 10 %) — это **не** технический сбой, а бизнес-результат.
  Статус `failed` фиксируется как финальный, webhook отправляется, retry не запускается.
- **Провал доставки webhook** не откатывает платёж: фиксируется в `webhook_status`
  (`pending` / `delivered` / `failed`) и `webhook_attempts`.

## Демонстрация сценариев

Поле `metadata.force` управляет поведением эмуляции шлюза.

| Сценарий | Как вызвать | Результат |
|---|---|---|
| Успех | `"metadata":{"force":"succeed"}` | `succeeded`, webhook доставлен |
| Отказ шлюза | `"metadata":{"force":"decline"}` | `failed`, webhook доставлен, DLQ пуст |
| Технический сбой | `"metadata":{"force":"crash"}` | 3 попытки (2s, 4s) → `payments.dlq` |
| Ретраи webhook | `"webhook_url":"http://webhook:9000/flaky"` | 500→500→200, `webhook_attempts=3` |
| Webhook недоступен | `"webhook_url":"http://webhook:9000/fail"` | платёж обработан, `webhook_status=failed` |
| Брокер лежит | `docker compose stop rabbitmq` | API отвечает 202, события копятся в outbox |

### Примеры запросов

Успешный платёж:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-key" -H "Idempotency-Key: ok-1" \
  -H 'Content-Type: application/json' \
  -d '{"amount":"100.00","currency":"RUB","metadata":{"force":"succeed"},"webhook_url":"http://webhook:9000/hook"}'
```

Отказ шлюза (10 %):

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-key" -H "Idempotency-Key: dec-1" \
  -H 'Content-Type: application/json' \
  -d '{"amount":"100.00","currency":"USD","metadata":{"force":"decline"},"webhook_url":"http://webhook:9000/hook"}'
```

Технический сбой → DLQ:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-key" -H "Idempotency-Key: crash-1" \
  -H 'Content-Type: application/json' \
  -d '{"amount":"100.00","currency":"EUR","metadata":{"force":"crash"},"webhook_url":"http://webhook:9000/hook"}'
```

### Проверка состояния

```bash
# Статусы событий в outbox
docker compose exec db psql -U app -d payments \
  -c "SELECT status, count(*) FROM outbox GROUP BY 1;"

# Сообщения в очередях
docker compose exec rabbitmq rabbitmqctl list_queues name messages

# Логи консьюмера (время между попытками)
docker compose logs -f consumer
```

После `crash-1` в логах видны интервалы ~2s и ~4s, затем сообщение появляется
в очереди `payments.dlq` (RabbitMQ UI → Queues → `payments.dlq` → Get messages).

## Миграции

Alembic работает в async-режиме через `asyncpg`. Контейнер `migrate` — one-shot сервис
с `restart: "no"`; `api`, `outbox` и `consumer` стартуют по условию
`service_completed_successfully`.

### Команды

```bash
# Генерация новой ревизии на хосте (порт 5432 опубликован)
uv run alembic revision --autogenerate -m "описание"

# Применить миграции
docker compose run --rm migrate alembic upgrade head

# Текущая ревизия
docker compose run --rm migrate alembic current

# История ревизий
docker compose run --rm migrate alembic history --verbose

# Откат на одну ревизию назад
docker compose run --rm migrate alembic downgrade -1

# Проверка: нет ли расхождений между моделью и схемой БД
docker compose run --rm migrate alembic check

# Оффлайн-режим: выгрузить SQL для ревью
docker compose run --rm migrate alembic upgrade head --sql
```

### Параллельный запуск и advisory lock

В `alembic/env.py` используется `pg_advisory_lock` — если несколько инстансов
`migrate` стартуют одновременно (CI, несколько реплик), второй дождётся первого,
а не начнёт DDL параллельно.

### Добавление значения ENUM

`ALTER TYPE … ADD VALUE` нельзя выполнять внутри транзакции до PostgreSQL 12,
а с 12-го — можно, но новое значение **нельзя использовать в той же транзакции**.
Поэтому миграция оборачивается в `autocommit_block`:

```python
def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE currency ADD VALUE IF NOT EXISTS 'GBP'")
```

`DROP VALUE` в PostgreSQL нет: значение либо остаётся deprecated, либо тип
пересоздаётся (создать `currency_new`, перевести колонки `USING currency::text::currency_new`,
дропнуть старый, `ALTER TYPE … RENAME TO currency`). После пересоздания типа
**перезапусти сервисы** — asyncpg кэширует кодеки типов на соединениях.

## Тесты

Проект покрыт unit- и integration-тестами (testcontainers для реального Postgres и RabbitMQ).

```bash
# Быстрые unit-тесты (Docker не требуется)
uv run pytest -m "not integration" -v

# Полный набор (поднимает контейнеры Postgres 18 и RabbitMQ)
uv run pytest -v

# Только API-сценарии
uv run pytest tests/test_api_payments.py -v

# Линтеры и проверка типов
uv run ruff check . && uv run ruff format --check .
uv run mypy
```

**Что покрыто:** аутентификация, валидация схем, идемпотентность (повтор, гонка запросов), атомарность outbox (rollback не оставляет следов), публикация и переживание падения брокера, возврат сообщения по TTL, попадание в DLQ, ретраи и HMAC-подпись webhook, паритет типов ENUM между моделью и БД.

## Структура проекта

```
app/
  main.py · consumer.py · outbox_worker.py
  core/          config, exceptions, retry
  db/            base, models, session
  schemas/       payments, events
  repositories/  payments, outbox
  services/      payments, processing, gateway, webhook
  broker/        topology, core, publisher, handlers
  api/           security, deps, payments, health
alembic/         # миграции и env.py
tests/           # unit и integration тесты
scripts/         # webhook_receiver.py (демо-приёмник)
pyproject.toml · uv.lock · .env.example
Dockerfile · docker-compose.yml
```