# Масштабирование, HA и восстановление

## 1. Подход

Сначала применяются вертикальный right-sizing, connection pooling, индексы, batch, time partitioning, кэш и read replicas. Шардирование добавляется только при подтверждённом bottleneck: оно дорого в эксплуатации и при отсутствии SRE повышает риск больше, чем доступность.

Планирование ведётся с запасом 30–50% над измеренным пиком: 18k RPS через 3 месяца и 50k RPS через год. Решение о расширении принимается по нагрузочному тесту и тренду за 30 дней, а не только по CPU.

## 2. Репликация

### PostgreSQL

- Для Bidding, Campaign, Statistics и Finance: один **primary** для записи и минимум один **standby** в другой availability zone; physical streaming replication.
- Finance и критичный Bidding: synchronous standby в пределах региона там, где latency-тест подтверждает бюджет; `synchronous_commit=remote_write` или `on` выбирается по RPO/latency. Остальные могут использовать asynchronous standby.
- Автоматический failover — Patroni/managed PostgreSQL, consensus store в трёх fault domains. Fencing старого primary обязателен, иначе возможен split-brain.
- Read replicas обслуживают только stale-tolerant queries. Analytics не читает OLTP replicas: его query-store — ClickHouse. Временная read replica общей AS-IS PostgreSQL допустима лишь на миграционной фазе, чтобы убрать тяжёлые запросы.
- Реплика не является backup: ошибочное удаление и corruption реплицируются.

### ClickHouse

- На старте: один shard × две replicas, `ReplicatedMergeTree`, ClickHouse Keeper из трёх узлов/managed equivalent.
- Distributed table маршрутизирует запросы. Добавление shard — online migration/rebalance с контролем дубликатов.
- Для dashboard допустима eventual consistency; ingestion повторяется из Kafka/архива.

### Redis и Kafka

- Redis: для 18k RPS — primary + replica по shard и Sentinel/managed failover; для 50k — Redis Cluster минимум 3 primary shards + replicas после замеров памяти/throughput.
- Kafka: минимум 3 brokers, replication factor 3, `min.insync.replicas=2`, producer `acks=all`, idempotent producer, unclean leader election disabled.
- Redis и Kafka не заменяют SoT/backup Finance.

## 3. Партиционирование и шардирование

| Сервис | Сначала | Ключ будущего шарда | Почему и ограничения | Условие включения |
|---|---|---|---|---|
| Bidding | Hash-partitioning больших audit/decision tables; Redis partitioning | `hash(auction_id)` для решений; Campaign read-модель — `hash(campaign_id)` | Равномерный поток. Все данные одного auction остаются вместе; глобальные выборки идут в Analytics | Primary >70% CPU/IO 15 мин при оптимизированных запросах, p95 DB выше бюджета либо объём > одного узла; тест показывает ≥2× выигрыш |
| Campaign | PostgreSQL partitioning по `tenant_id`/служебным hash partitions | `hash(advertiser_id/tenant_id)` | Кампании рекламодателя и транзакции локальны. Нельзя шардировать по status/date — skew и межшардовые updates | >10k TPS commands, рабочий набор не помещается в RAM/IOPS или окно обслуживания не выполняется |
| Statistics | Native range partitions по `event_date`, subpartition/hash при необходимости | Сначала `event_date`; при distributed ingest — `hash(event_id)` внутри временного bucket | Удаление retention через detach/drop partition; равномерная запись. Запросы по campaign обслуживает Analytics | Batch insert не держит lag SLO, storage/maintenance window превышены, один primary устойчиво saturated |
| Analytics | ClickHouse partition by month; order by `(tenant_id, campaign_id, event_date, event_time)` | `hash(tenant_id)` либо `hash(campaign_id)` | Распараллеливает tenant/campaign queries; hot tenant при необходимости получает dedicated shard | Query p95/merge backlog нарушает SLO после projections/materialized views; shard >70% CPU/disk/IO |
| Finance | Табличные time partitions для ledger; без distributed shard на старте | `hash(account_id)` с shard map | Все проводки одного account локальны. Межаккаунтные операции требуют Saga/clearing account; resharding под строгим контролем | Один HA-кластер не выдерживает прогноз с 2× запасом или storage/backup window; после доказанного operational readiness |

Шардирование не включается «на всякий случай». Для Finance оно последнее: сложность cross-shard reconciliation и resharding повышает финансовый риск. Shard map версионируется; переход использует copy → dual-read verification → короткий fenced cutover, а не неконтролируемый dual-write.

## 4. CQRS и маршрутизация

- **Campaign command:** API → Campaign primary. Query: собственная replica для кабинета; Bidding читает отдельную проекцию по событиям.
- **Bidding command/decision:** stateless workers + immutable in-memory L1 snapshot; Redis L2 / read-модель — background. Решение и outbox пишутся асинхронно/вне response path там, где бизнес допускает. Отправка bid response не ждёт Analytics/Statistics.
- **Statistics command:** Kafka consumer batches → Statistics primary. Operational reads — standby.
- **Analytics query:** только ClickHouse read-модель; ingestion consumer масштабируется до числа partitions.
- **Finance command:** только Finance primary/shard owner. Query баланса после write — primary/read-your-writes; отчётные запросы — Analytics projection.

Для CQRS явно измеряются projection lag и возраст snapshot. При превышении 5 s для Campaign→Bidding новые/изменённые кампании временно не участвуют либо используется последняя заведомо корректная версия; нельзя «угадывать» финансовый лимит.

## 5. Capacity и триггеры

| Компонент | Метрики | Действие |
|---|---|---|
| PostgreSQL | CPU/IO, active connections, lock wait, p95 query, replication lag, WAL/day | Индексы/пул/replica → vertical scale → partition → shard |
| ClickHouse | rows/s, query p95, merge queue, parts/partition, disk utilization | Batch size/materialized view → replica → shard |
| Redis | hit ratio, p99, memory, evictions, ops/s, hot keys | TTL/key redesign → memory/replica → shards |
| Kafka | ingress MB/s, partition utilization, ISR, consumer lag | Consumer instances → partitions → brokers |

Нельзя уменьшить число Kafka partitions без нового topic; первоначальное количество выбирается по throughput-тесту с запасом, но не завышается из-за overhead. Добавление partitions меняет распределение ключей и не сохраняет глобальный порядок, поэтому порядок гарантируется только внутри partition для одного key.

## 6. RPO/RTO

Цели относятся к региональному одиночному отказу. Полная потеря региона имеет отдельные, более слабые DR-цели до внедрения multi-region.

| Сервис | RPO | RTO | Механизм |
|---|---:|---:|---|
| Bidding | ≤ 1 мин для конфигурации/аудита; 0 для committed при synchronous standby | ≤ 5 мин | HA PostgreSQL, Redis rebuild, stateless failover; no-bid при отсутствии безопасной конфигурации |
| Campaign | ≤ 5 мин | ≤ 30 мин | Standby + PITR; Bidding продолжает последнюю валидную snapshot с age guard |
| Statistics | ≤ 5 мин в БД, фактически 0 для acknowledged Kafka records в retention | ≤ 60 мин | Replay Kafka в восстановленную БД; object archive для длинного горизонта |
| Analytics | ≤ 15 мин для проекции | ≤ 4 ч | ClickHouse replicas + replay Kafka/Parquet; отчётность может деградировать без RTB outage |
| Finance | **0 для подтверждённых операций в регионе** | ≤ 15 мин | Synchronous standby, WAL archive/PITR, idempotent retry и reconciliation |

Для полной потери региона до year-1 DR: Bidding/Campaign RPO ≤ 15 мин, RTO ≤ 60 мин; Statistics RPO ≤ 15 мин, RTO ≤ 4 ч; Analytics RPO ≤ 24 ч, RTO ≤ 8 ч; Finance RPO ≤ 5 мин, RTO ≤ 2 ч. После внедрения multi-region цели пересматриваются, но Finance не становится multi-writer без формально спроектированного ownership account/shard.

## 7. Backup и restore

### Политика

- PostgreSQL: ежедневный base backup, непрерывное WAL archiving, PITR; 35 дней hot retention, 12 ежемесячных копий. Finance — дополнительно immutable/WORM-копия ledger export.
- ClickHouse: ежедневный incremental backup metadata/data в object storage; 30 дней. Основной путь rebuild — Kafka + Parquet archive.
- Redis: AOF/RDB нужны для ускорения рестарта, но кэш должен полностью восстанавливаться из SoT.
- Kafka: RF=3 защищает от broker failure, но не является backup. Критичные raw events экспортируются в immutable object storage; schemas и topic configuration backup-ятся отдельно.
- Копии шифруются отдельным KMS key, доступны backup-role, размещаются в другом account/регионе и защищены object lock.

## 8. 12-factor

| Принцип | Реализация |
|---|---|
| Codebase / dependencies | Один репозиторий/артефакт на сервис; lockfile, SBOM и immutable image |
| Config | DSN, topic names, TTL и feature flags из environment/config service; секреты не в Git |
| Backing services | PostgreSQL, Redis, Kafka, ClickHouse подключаются как заменяемые ресурсы через URL/credentials |
| Build, release, run | Разделены; schema migration — versioned release step с expand/contract |
| Processes | Stateless Bidding/API; состояние только во внешних stores. Consumers переживают restart через offsets + inbox |
| Port binding | Каждый сервис экспортирует API/health/metrics через порт |
| Concurrency | Масштабирование process/container; Kafka consumers ограничены числом partitions |
| Disposability | Graceful shutdown: прекратить intake, завершить transaction/batch, commit offset, закрыть pool |
| Dev/prod parity | Одинаковые engine major versions и Avro schemas; production-like load/staging |
| Logs | Structured JSON в stdout, correlation/trace/event IDs; без локальных log files |
| Admin processes | Миграции, replay и reconciliation — одноразовые auditable jobs из того же release |
