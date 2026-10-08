# Payment processing service

Микросервис принимает запросы на оплату, асинхронно проводит их через эмулятор платёжного шлюза и сообщает результат на webhook клиента.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL 16, RabbitMQ 4.3 через FastStream, Alembic, Docker Compose.

## Запуск

```bash
docker compose up -d --build --wait
./scripts/demo.sh
```

Поднимаются `postgres`, `rabbitmq`, `migrate` (применяет миграции и завершается), `api`, `consumer` и `webhook-receiver`. Последний это маленький получатель уведомлений для проверки: всё, что ему пришло, видно на `/received`.

`.env` не обязателен, значения по умолчанию заданы в compose. Переопределить порты или ключ можно через `.env`, пример в `.env.example`. Порты слушают только `127.0.0.1`.

| Что | Адрес |
|---|---|
| API | http://127.0.0.1:8000 |
| Swagger | http://127.0.0.1:8000/docs (ключ вводится через Authorize) |
| RabbitMQ UI | http://127.0.0.1:15672, guest / guest |
| Получатель webhook | http://127.0.0.1:9000/received |

API-ключ по умолчанию `dev-api-key`.

## API

Все запросы к `/api/v1/*` и `/health` требуют заголовок `X-API-Key`.

### Создание платежа

```bash
curl -i -X POST http://127.0.0.1:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order 1001",
        "metadata": {"order_id": 1001},
        "webhook_url": "http://webhook-receiver:9000/ok"
      }'
```

```
HTTP/1.1 202 Accepted

{"payment_id": "b230e633-1f02-4cb5-bfb3-a42ad0e12d78", "status": "pending", "created_at": "2026-10-08T08:37:54.613222Z"}
```

`amount` лучше передавать строкой, до двух знаков после запятой. `description` и `metadata` необязательны. `webhook_url` указан адресом внутри docker-сети, потому что запрос к нему делает consumer. Чтобы посмотреть повторы и DLQ, укажите `http://webhook-receiver:9000/fail`, этот адрес всегда отвечает 500.

### Получение платежа

```bash
curl http://127.0.0.1:8000/api/v1/payments/b230e633-1f02-4cb5-bfb3-a42ad0e12d78 -H "X-API-Key: dev-api-key"
```

```json
{
  "id": "b230e633-1f02-4cb5-bfb3-a42ad0e12d78",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order 1001",
  "metadata": {"order_id": 1001},
  "status": "succeeded",
  "failure_reason": null,
  "idempotency_key": "order-1001",
  "webhook_url": "http://webhook-receiver:9000/ok",
  "webhook_status": "delivered",
  "webhook_attempts": 1,
  "created_at": "2026-10-08T08:37:54.613222Z",
  "processed_at": "2026-10-08T08:37:58.984511Z",
  "updated_at": "2026-10-08T08:37:59.021280Z"
}
```

### Webhook

```
POST <webhook_url>
X-Event-Id: 339964e7-7900-52c9-992b-6dd0714a6c34
X-Event-Type: payment.succeeded

{
  "event_id": "339964e7-7900-52c9-992b-6dd0714a6c34",
  "event_type": "payment.succeeded",
  "payment_id": "b230e633-1f02-4cb5-bfb3-a42ad0e12d78",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order 1001",
  "metadata": {"order_id": 1001},
  "failure_reason": null,
  "created_at": "2026-10-08T08:37:54.613222+00:00",
  "processed_at": "2026-10-08T08:37:58.984511+00:00"
}
```

Успехом считается любой ответ 2xx. Редиректы не выполняются.

### Коды ответов

| Ситуация | Ответ |
|---|---|
| нет или неверный `X-API-Key` | 401 |
| нет `Idempotency-Key` (или длиннее 255 символов) | 400 |
| невалидное тело | 422 |
| тот же `Idempotency-Key` с другим телом | 422 |
| повтор того же запроса | 202, исходный ответ и заголовок `Idempotent-Replayed: true` |
| платёж не найден | 404 |

Коды для идемпотентности взяты из драфта IETF [Idempotency-Key HTTP Header](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/): 400 без ключа, 422 при повторе ключа с другим телом.

## Как устроено

```
POST /payments ──> одна транзакция: INSERT payments + INSERT outbox
                                         │
                   outbox-релей (фоновая задача api) ── publish + confirm ──> payments.new
                                                                                 │
                                                                             consumer
                                                     эмулятор шлюза: 2-5 c, 90% succeeded, 10% failed
                                                     UPDATE статуса ──> POST webhook
                                                                                 │
                        ошибка, попытка 1 ──> payments.retry.2s (TTL 2 c) ──> payments.new
                        ошибка, попытка 2 ──> payments.retry.4s (TTL 4 c) ──> payments.new
                        ошибка, попытка 3 ──> reject ──> payments.dlx ──> payments.dlq
```

**Outbox.** Платёж и событие `payment.created` пишутся в одной транзакции, поэтому событие не теряется, если процесс упадёт сразу после коммита. Релей выбирает неопубликованные строки через `SELECT ... FOR UPDATE SKIP LOCKED`, публикует их с подтверждением брокера (publisher confirms, `mandatory`) и только после подтверждения ставит `published_at`. Если сообщение не попало ни в одну очередь (например, пропал binding), брокер его возвращает, релей получает исключение и откладывает событие с растущей паузой до 60 с. Ошибка и число попыток видны в самой строке outbox.

**Идемпотентность на входе.** `idempotency_key` уникален в таблице `payments`. Вставка идёт через `INSERT ... ON CONFLICT DO NOTHING`, так что из двух одновременных запросов с одним ключом платёж создаёт ровно один, второй получает тот же ответ. В строке хранится sha256 тела запроса: тот же ключ с другим телом отклоняется, а не возвращает чужой платёж. Сумма перед хэшем нормализуется, `"100"` и `"100.00"` это один запрос.

**Идемпотентность обработки.** Публикация из outbox даёт гарантию at-least-once, поэтому одно событие может прийти дважды. Результат списания записывается условным `UPDATE ... WHERE status = 'pending'`, поэтому статус меняется ровно один раз. Состояние доставки webhook (`pending`, `delivered`, `failed`) меняется только из `pending`, повторно пришедшее событие по уже завершённой доставке ничего не отправляет.

Каждый шаг идёт в своей короткой транзакции: вызов шлюза и HTTP-запрос к получателю выполняются вне транзакций и не держат соединение с базой.

**Повторы.** Отказ шлюза (10%) это результат платежа, а не ошибка: статус `failed`, клиент получает webhook `payment.failed`. Повторяются ошибки доставки webhook (сеть, таймаут, любой ответ кроме 2xx) и ошибки инфраструктуры (база недоступна). Всего три попытки: сразу, через 2 с и через 4 с. Задержка сделана временем жизни сообщения в отдельной очереди без потребителей: когда TTL истекает, брокер сам возвращает сообщение в `payments.new`. Очередь на каждую задержку отдельная, иначе сообщение с длинной задержкой в голове общей очереди задерживало бы остальные. Номер попытки едет в заголовке `x-attempt`.

**DLQ.** После третьей неудачной попытки сообщение отклоняется и через `payments.dlx` попадает в `payments.dlq`. Туда же уходят сообщения, которые не разбираются, и события по несуществующему платежу. Все очереди quorum со стратегией dead-lettering `at-least-once`: у классических очередей пересылка в DLX идёт в режиме at-most-once, и сообщение могло бы потеряться по пути в DLQ или из retry-очереди обратно.

**Аутентификация.** Статический ключ в `X-API-Key`, сравнение через `secrets.compare_digest`. Swagger (`/docs`, `/openapi.json`) открыт для удобства проверки, отключается переменной `DOCS_ENABLED=false`.

### Гарантии и их границы

- Принятый платёж (ответ 202) не потеряется: событие лежит в outbox, пока брокер его не подтвердит.
- Итоговый статус платежа фиксируется один раз.
- Webhook доставляется по схеме at-least-once. Дубль возможен, если получатель принял запрос, а ответ до нас не дошёл. Поэтому у каждого уведомления постоянный `X-Event-Id` (один и тот же на всех повторах), получателю стоит дедуплицировать по нему.
- При повторной доставке события шлюз может быть вызван второй раз. Эмулятор внешних эффектов не имеет. Настоящему шлюзу передавался бы id платежа как ключ идемпотентности.

### Разбор DLQ

В RabbitMQ UI: Queues, `payments.dlq`, Get messages. Заголовок `x-event-id` совпадает с `outbox.id`, по нему находится платёж, а причина последней ошибки лежит в `payments.webhook_last_error`. Автоматически из DLQ ничего не переигрывается: это отдельное решение после того, как причина устранена (например, перенос сообщений обратно в `payments.new` через shovel и сброс `webhook_status` в `pending`).

## Конфигурация

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DATABASE_URL` | задан в compose | PostgreSQL, драйвер asyncpg |
| `RABBITMQ_URL` | задан в compose | RabbitMQ |
| `API_KEY` | `dev-api-key` | ключ для `X-API-Key` |
| `DOCS_ENABLED` | `true` | Swagger и OpenAPI |
| `GATEWAY_MIN_DELAY`, `GATEWAY_MAX_DELAY` | `2`, `5` | задержка эмулятора, секунды |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных списаний |
| `CONSUMER_PREFETCH` | `10` | сколько сообщений consumer обрабатывает одновременно |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL` | `20`, `0.5` | пачка и период опроса outbox |
| `WEBHOOK_CONNECT_TIMEOUT`, `WEBHOOK_READ_TIMEOUT`, `WEBHOOK_DEADLINE` | `2`, `5`, `10` | таймауты запроса к получателю |

Число попыток и задержки повторов зафиксированы в `app/messaging.py`: они заданы аргументами очередей, а аргументы существующей очереди в RabbitMQ поменять нельзя.

## Тесты

```bash
pip install -r requirements-dev.txt
make lint
make test-unit           # без Docker
make test-integration    # поднимает отдельный стек payments-test со своими портами и томами
```

Интеграционные тесты проверяют через настоящие PostgreSQL и RabbitMQ: ключ и коды ответов, повтор запроса и 20 одновременных запросов с одним `Idempotency-Key`, обработку и тело webhook, ровно три попытки с паузами 2 и 4 с и попадание события в DLQ, повторную доставку того же события, битое сообщение, приём платежа при остановленном брокере, событие без маршрута (снятый binding) и падение consumer посреди обработки (`docker compose kill`). Чтобы тесты были предсказуемыми, в тестовом стеке включён `GATEWAY_TEST_HOOKS`: исход и задержку эмулятора можно задать в `metadata.emulator`.

## Что добавил бы для продакшена

- Подпись webhook (HMAC) и проверку `webhook_url` на внутренние адреса.
- Разделение ошибок получателя на временные и постоянные (4xx кроме 408 и 429 сразу в DLQ).
- Очистку опубликованных строк outbox и LISTEN/NOTIFY вместо опроса.
- Метрики: размер outbox, глубина очередей, доля неуспешных доставок.
- Вынос релея в отдельный процесс при масштабировании api.
