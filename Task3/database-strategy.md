# Стратегия данных и Database per Service

## 1. Принципы

1. Каждый сервис — единственный владелец своей модели записи, миграций, учётных данных, SLO и восстановления.
2. Database per Service означает логическую изоляцию, а не обязательный отдельный сервер: на старте допустим общий PostgreSQL-кластер с отдельными databases/roles, но без cross-database join и общих таблиц. Finance и Bidding размещаются на разных кластерах или хотя бы разных ресурсных пулах.
3. Другие сервисы получают данные через versioned API либо интеграционные события. Изменение чужих таблиц и CDC как публичный контракт запрещены.
4. Для межсервисной согласованности используются Saga/process manager, outbox и идемпотентные потребители; 2PC не применяется.
5. Источником истины является база owning-сервиса. Kafka — транспортируемый журнал интеграции, Redis — производный кэш, ClickHouse — производная аналитическая read-модель.

## 2. Матрица выбора

| Сервис | Собственное хранилище | Источник истины и данные | Почему |
|---|---|---|---|
| **Bidding** | PostgreSQL + локальная read-модель; Redis как shared L2 | PostgreSQL: bid policies, версии алгоритмов, результаты/решения, idempotency и outbox. Горячие конфигурации Campaign копируются как read-модель и не становятся owned-данными | ACID для собственной конфигурации и аудита решений; PostgreSQL знаком команде. Hot path читает только immutable in-memory L1; Redis/PostgreSQL — background warm/recovery, не sync Campaign DB |
| **Campaign** | PostgreSQL | Кампании, creatives, targeting rules, bid settings, статусы и версии конфигурации | Связанные CRUD-данные, ограничения и транзакции; удобны FK/unique/check constraints |
| **Statistics** | PostgreSQL, партиционированный по времени; объектное хранилище для дешёвого архива | Нормализованные сырые click/impression/bid-result события и deduplication state | Batch insert и умеренные operational-запросы; Kafka сглаживает пики. Старые immutable-данные выгружаются в Parquet |
| **Analytics** | ClickHouse | Производные факты и агрегаты для CTR/CPC/CPA, временных рядов и дашбордов | Колоночное хранение, сжатие и параллельный OLAP не влияют на OLTP. Не является SoT исходных транзакций |
| **Finance** | Отдельный PostgreSQL | Балансы, double-entry ledger, reservations, charges, refunds, invoices, idempotency и outbox | Денежные инварианты требуют ACID, constraints, serializable/locking для конфликтующих операций и полного аудита |

## 3. Владение данными

| Доменный объект | Владелец записи | Разрешённые читатели | Способ распространения |
|---|---|---|---|
| Campaign, creative, targeting, max bid | Campaign | Bidding, Analytics | `campaign.configuration.v1`; Bidding materializes read-модель |
| Bid policy/decision | Bidding | Statistics, Analytics | `auction.bid-result.v1` |
| Impression/click | Statistics после приёма события | Analytics, Finance | Исходное событие через Kafka (`ad.impression.v1` / `ad.click.v1`); billing стартует здесь; нормализованная статистика через API/экспорт |
| Баланс, reservation, charge, refund, invoice | Finance | Campaign UI, Analytics | Finance API для актуального состояния; `finance.ledger-entry.v1` для проекций |
| Отчётные агрегаты | Analytics | Dashboard/API | Analytics API |

Analytics не выполняет SQL в production-БД других сервисов. Finance не рассчитывает остаток по Kafka, Redis или ClickHouse: баланс и неизменяемые проводки фиксируются в одной локальной транзакции Finance PostgreSQL.

## 4. Модели и инварианты

### Bidding

- `bid_policy(policy_id, version, status, algorithm, updated_at)`.
- `auction_decision(auction_id, request_id, campaign_id, price, currency, policy_version, decided_at)`.
- `inbox(event_id, processed_at)` и `outbox(event_id, aggregate_id, type, payload, created_at, published_at)`.
- `auction_id`/`request_id` уникальны для дедупликации повторов DSP.
- Снимок Campaign read-модели содержит `campaign_id`, `version`, eligibility, targeting, creative reference и лимиты; событие с меньшей версией игнорируется.

### Campaign

- Aggregate `campaign` управляет статусом, schedule и версией.
- Creative и targeting изменяются в одной транзакции с increment `campaign.version` и записью outbox.
- Денежный баланс не хранится здесь; допустим только display-only snapshot с временем обновления.

### Statistics

- Уникальный `event_id`; для природной дедупликации также индекс `(event_type, source, source_event_id)`.
- Таблицы range-partitioned по `event_date`; новые секции создаются заранее.
- Событие считается обработанным только после commit batch + inbox в одной транзакции, затем offset Kafka подтверждается.

### Analytics

- `ReplacingMergeTree` по `event_id, event_version` либо агрегирующие таблицы/Materialized Views.
- Дедупликация не должна полагаться только на фоновые merge: запросы учитывают версию, а ingestion ведёт контрольный watermark.
- ClickHouse допускает задержку секунд/минут; актуальные деньги всегда запрашиваются у Finance API.

### Finance

- Double-entry ledger: сумма debit и credit по transaction равна нулю; записи не обновляются, исправление — компенсирующей проводкой.
- `business_operation_id` уникален, суммы хранятся целым числом minor units (`BIGINT`) плюс ISO currency.
- Изменение ledger, materialized balance и outbox выполняется в одной ACID-транзакции.
- Для одного account операции сериализуются row lock/optimistic version. Isolation выбирается нагрузочными тестами; критичные переводы — `SERIALIZABLE` с bounded retry.

## 5. Консистентность и CQRS

| Поток | Модель консистентности | Решение |
|---|---|---|
| Campaign command | Strong внутри aggregate | Commit в Campaign PostgreSQL + outbox |
| Campaign → Bidding | Eventual, целевое отставание ≤ 5 s | Версионированное событие, Bidding inbox/read-модель, cache invalidation |
| RTB decision | Snapshot consistency | Один versioned snapshot на весь аукцион; при stale/отсутствующей модели — no-bid |
| Impression/click → Statistics/Analytics | Eventual, at-least-once | Kafka + deduplication |
| Charge/reservation | Strong внутри Finance | Idempotent Finance command и локальная ACID-транзакция |
| Finance → Analytics | Eventual | Ledger events без права менять Finance SoT |

Command side остаётся в PostgreSQL владельца. Query side создаётся только там, где это снижает latency/изоляцию: Bidding read-модель кампаний и Analytics ClickHouse. Для админских CRUD Campaign читает свой primary или synchronous-enough standby; нельзя показывать пользователю «успешное изменение» из отставшей реплики сразу после записи без read-your-writes token/маршрутизации на primary.

## 6. Миграция из общей базы

1. Зафиксировать владельцев таблиц и запретить новые междоменные FK/запросы.
2. Создать целевую БД и выполнить snapshot + catch-up через контролируемый CDC только как временный migration mechanism.
3. Перевести запись на API нового сервиса; события публиковать outbox-ом.
4. Сравнивать counts, checksums и доменные инварианты; выполнить shadow-read.
5. Переключить чтение, остановить CDC, оставить старые таблицы read-only на период отката и затем удалить по change approval.

Dual-write из приложения без outbox не используется: частичный успех породит расхождение.

## 7. Безопасность и эксплуатация

- Отдельные роли `service_rw`, `migration_owner`, `backup`, read-only; credentials выдаются через secret manager и ротируются.
- TLS in transit, encryption at rest, аудит Finance DDL/DML и маскирование PII.
- Миграции backward-compatible: expand → migrate/backfill → contract; один deploy не требует синхронного обновления всех consumers.
- Обязательные метрики: saturation, locks, replication lag, WAL growth, slow queries, storage forecast, ClickHouse merge backlog, Redis evictions и Kafka lag.
