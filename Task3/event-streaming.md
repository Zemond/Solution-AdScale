# Архитектура потоковой обработки

## 1. Роль Kafka

Kafka отделяет RTB response path от Statistics, Analytics и Finance, сглаживает пики записи и позволяет replay. Гарантия доставки — **at-least-once**; end-to-end exactly-once между Kafka и PostgreSQL не предполагается. Корректность достигается outbox, уникальными IDs, inbox/deduplication и атомарными локальными транзакциями.

Kafka не является источником истины для денег. Подтверждённые ledger entries, balances и reservations существуют в Finance PostgreSQL. Kafka содержит их интеграционные представления в пределах retention.

## 2. Кластер и параметры надёжности

- 3 brokers в трёх fault domains, KRaft/managed Kafka.
- Replication factor 3, `min.insync.replicas=2`, producer `acks=all`, idempotence enabled.
- `unclean.leader.election.enable=false`; compression `zstd`/`lz4`.
- TLS, SASL, ACL по принципу least privilege: producer пишет только свои topics, consumer group читает только нужные.
- Schema Registry работает в HA; compatibility mode `BACKWARD_TRANSITIVE`.
- Quotas разделяют critical topics от analytics/replay. Lag отчётности не блокирует bidding producer.

В первые 3 месяца предпочтителен managed Kafka, если бюджет позволяет: при отсутствии SRE self-hosting критичного кластера обычно дороже с учётом on-call. Если используется self-hosted, KRaft, IaC, автоматизированные backup конфигурации и runbooks обязательны.

## 3. Каталог topics

Имена включают домен, событие и major contract version. Число partitions — стартовая гипотеза; финальное значение задаёт throughput test с размером реального сообщения.

| Topic | Producer | Partition key | Partitions 18k / 50k | Retention | Consumers |
|---|---|---|---:|---:|---|
| `campaign.configuration.v1` | Campaign outbox relay | `campaign_id` | 6 / 12 | compact + 7 d delete | Bidding projection, Analytics dimensions |
| `auction.bid-result.v1` | Bidding outbox relay | `auction_id` | 12 / 24 | 7 d | Statistics, Analytics |
| `ad.impression.v1` | Delivery/event gateway | `campaign_id` | 12 / 24 | 14 d | Statistics, Analytics, Finance rating |
| `ad.click.v1` | Delivery/event gateway | `campaign_id` | 12 / 24 | 30 d | Statistics, Analytics, Finance rating/fraud |
| `finance.charge-request.v1` | Finance rating adapter | `account_id` | 12 / 24 | 14 d | Finance command consumer |
| `finance.ledger-entry.v1` | Finance outbox relay | `account_id` | 6 / 12 | 90 d + object archive | Analytics finance projection, audit export |
| `finance.allowance.v1` | Finance outbox relay | `account_id` | 6 / 12 | compact + 7 d delete | Bidding allowance projector |

`campaign_id` сохраняет порядок событий одной кампании и естественно распределяет основной поток. Если одна кампания становится hot key (>10% partition throughput), ключ для click/impression меняется в новой major-версии на `campaign_id:event_id_bucket`; агрегирование по кампании тогда не рассчитывает на порядок. Для Finance ключ `account_id` обязателен: операции счёта последовательно попадают в один partition, хотя PostgreSQL всё равно остаётся арбитром конкуренции.

Retention Kafka покрывает operational replay, но не юридическое хранение. Raw events и ledger export непрерывно выгружаются в versioned immutable object storage (Parquet/Avro + manifest/checksum) по отдельной политике retention.

## 4. Контракт Avro

Envelope обязателен для всех событий:

```json
{
  "type": "record",
  "name": "ImpressionOccurred",
  "namespace": "com.adscale.ad.v1",
  "fields": [
    {"name": "event_id", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "event_type", "type": "string"},
    {"name": "occurred_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "produced_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "producer", "type": "string"},
    {"name": "trace_id", "type": ["null", "string"], "default": null},
    {"name": "schema_version", "type": "int", "default": 1},
    {"name": "auction_id", "type": "string"},
    {"name": "campaign_id", "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "source_event_id", "type": "string"},
    {"name": "price_minor", "type": ["null", "long"], "default": null},
    {"name": "currency", "type": ["null", "string"], "default": null}
  ]
}
```

Правила:

- Schema ID передаётся wire format Schema Registry; `schema_version` — доменная версия, а не замена registry ID.
- Денежные суммы — integer minor units + ISO 4217 currency, не floating point.
- PII не включается без необходимости; user identifiers pseudonymized, чувствительные поля шифруются/токенизируются.
- Backward-compatible evolution: добавление optional field с default. Rename/remove/type change — новый major topic или многофазная миграция.
- CI регистрирует candidate schema в test registry, проверяет compatibility и consumer contract tests до deploy.

## 5. Consumer groups

| Group | Topics | Обработка |
|---|---|---|
| `statistics-ingest-v1` | bid-result, impression, click | Micro-batch 100–1000 или ≤100 ms → Statistics PostgreSQL |
| `analytics-events-v1` | bid-result, impression, click | Batch insert → ClickHouse facts/materialized views |
| `analytics-dimensions-v1` | campaign configuration | Versioned campaign dimensions |
| `finance-rating-v1` | impression, click | Проверка billable event → charge request |
| `finance-command-v1` | charge request | ACID ledger/reservation/balance + inbox + outbox |
| `bidding-campaign-projection-v1` | campaign configuration | Bidding PostgreSQL read-модель + Redis invalidation |
| `bidding-allowance-projection-v1` | allowance | Allowance cache, только в пределах выданной Finance квоты |
| `audit-export-v1` | ledger entry | Immutable object archive |

Каждый экземпляр группы получает partitions эксклюзивно; масштабировать consumers сверх числа partitions бессмысленно. Static membership/cooperative rebalancing снижает паузы. Offset commit выполняется только после durable commit результата.

## 6. Outbox и публикация

Для Campaign, Bidding и Finance доменная запись и `outbox` создаются в одной PostgreSQL-транзакции. Relay читает непубликованные строки по `FOR UPDATE SKIP LOCKED`, отправляет событие с deterministic `event_id`, затем помечает запись опубликованной. Возможна повторная отправка между Kafka ack и отметкой `published_at`, поэтому consumer обязан дедуплицировать.

Outbox очищается после подтверждённой публикации и периода аудита; backlog/oldest record age мониторятся. CDC connector допустим как relay, но таблица outbox остаётся явным контрактом — Kafka не читает произвольные доменные таблицы.

Для внешних click/impression endpoint:

1. Проверить auth, размер, timestamp и stable `source_event_id`.
2. Produce в Kafka с `acks=all`; клиенту ответить после broker ack.
3. Повтор клиента с тем же ID допустим; downstream unique constraint/inbox устраняет дубль.

RTB bid response не должен ждать downstream consumers. Если audit bid-result обязателен до ответа, producer имеет жёсткий timeout и локальный bounded spool; при переполнении применяется явно согласованный fail mode, а не неограниченная память.

## 7. Идемпотентность и порядок

- Каждый event имеет globally unique `event_id`; бизнес-команда Finance — `business_operation_id`.
- PostgreSQL consumer в одной транзакции делает `INSERT inbox ... ON CONFLICT DO NOTHING`, применяет business change и записывает outbox.
- ClickHouse получает `event_id` + `event_version`; ingestion хранит source partition/offset и выполняет reconciliation. Фоновая дедупликация не считается мгновенной гарантией.
- Redis projectors применяют только событие с `version > current_version`.
- Порядок гарантирован только внутри partition. Consumers не полагаются на порядок разных campaign/account IDs.
- Event time и processing time различаются; Analytics использует watermark и окно допустимого опоздания, затем корректирует агрегат late event-ом.

## 8. Retry, DLQ и replay

Ошибки делятся на transient и permanent:

1. Transient (DB timeout, temporary network): 3–5 bounded retries с exponential backoff+jitter; затем retry topic.
2. Permanent (schema-valid, но нарушен бизнес-контракт): DLQ.
3. Невалидный Avro обычно отклоняется producer/serializer; raw ingress quarantine хранится отдельно с ограниченным доступом.

Topics: `<source>.retry-1m`, `<source>.retry-10m`, `<source>.dlq`. DLQ record содержит исходные bytes/reference, source topic/partition/offset, consumer group, error class, stack fingerprint, attempts и timestamps; PII маскируется.

- Retry topics: retention 3 дня; DLQ: 30 дней; owner и alert обязательны.
- DLQ не является «кладбищем»: runbook исправляет причину, проверяет schema/side effects и запускает controlled replay в исходный либо отдельный reprocess topic.
- Replay использует новую consumer group, rate limit и dry-run/count comparison. Finance replay всегда сохраняет тот же `business_operation_id`; side effects во внешнем payment gateway защищены idempotency key.

## 9. Backpressure и приоритеты

- Producers имеют bounded queue/buffer; consumers используют pause/resume и batch size limits.
- Critical `campaign.configuration` и Finance topics изолированы quotas от Analytics.
- При росте Analytics lag RTB продолжает работу; dashboard показывает `data_as_of`.
- Statistics/Finance alerts: lag age, а не только message count. Цели: Campaign→Bidding ≤5 s; Finance charge request ≤30 s; Statistics ≤60 s; Analytics ≤5 мин.
- Autoscaling consumers — по lag growth и processing rate; partitions увеличиваются до brokers, но только после capacity test.

## 10. Проверки

- Contract tests всех producer/consumer и backward-transitive compatibility.
- Fault injection: потеря broker, ISR=2, rebalance, duplicate/out-of-order event, poison pill, Schema Registry outage.
- End-to-end тест outbox crash в точках «до publish», «после publish до mark» и consumer crash «после DB commit до offset commit».
- Replay из retention/архива с проверкой counts, event IDs и Finance invariants.
- Нагрузочный тест с реальным средним/P99 размером событий при 18k и 50k events/s плюс burst 2×.
