# ADR-001: Kafka для потоковой обработки интеграционных событий

- **Статус:** принято
- **Дата:** 2026-07-27

## Контекст

AS-IS синхронно пишет клики и показы в общую PostgreSQL. Это конкурирует с аукционом и финансами. Целевому состоянию нужны независимые consumers, buffering, replay, versioned configuration projections и обработка не менее 18 000 событий/с с ростом до 50 000.

Команда не имеет выделенных SRE. Финансовая корректность не может зависеть от retention брокера или предположения об exactly-once.

## Решение

Использовать **managed Kafka** в трёхмесячной фазе. Самостоятельный Kafka в Kubernetes не вводить; Kubernetes не является prerequisite DSP-интеграции.

Kafka — транспорт и журнал интеграции, но не источник истины денег. Finance PostgreSQL хранит authoritative balances, reservations и double-entry ledger. Campaign PostgreSQL хранит authoritative campaign configuration.

### Надёжность

- минимум 3 brokers в разных fault domains;
- replication factor 3; это не число partitions;
- `min.insync.replicas=2`, `unclean.leader.election.enable=false`;
- для configuration, allowance, ledger, charge request и impression/click: `acks=all`, idempotent producer и bounded retries;
- consumers используют at-least-once, `event_id`, inbox/unique constraint и атомарную локальную транзакцию;
- число partitions выбирается по benchmark реального размера сообщений и требуемого consumer parallelism;
- schema registry и backward-compatible versioned contracts;
- producer acknowledgement имеет измеряемую задержку, но «non-blocking 1 ms» не является гарантией.

Decision telemetry допускает отдельный профиль потерь только после явной классификации как не финансово значимой, с bounded spool, метрикой drops и согласованным retention. Фактические impression/click и финансовые события используют durable profile.

### Публикация и миграция

Campaign, Bidding и Finance записывают доменное изменение и outbox в одной PostgreSQL-транзакции. Relay/CDC публикует outbox. Повтор между broker ack и отметкой `published` ожидаем, поэтому consumer дедуплицирует.

Application-level dual-write в БД и Kafka не используется. Kafka не заменяет backup: сырые события и ledger export архивируются в immutable object storage, а долгосрочная финансовая история остаётся в Finance DB.

## Альтернативы

- **RabbitMQ:** проще для очередей команд, но хуже соответствует длительному replay и нескольким независимым stream consumers.
- **NATS JetStream:** легче и быстрее в некоторых профилях, но команда получает менее знакомую экосистему для schema/replay tooling.
- **Cloud-native stream (Kinesis/Pub/Sub):** снижает эксплуатацию, но усиливает vendor lock-in; допустим при выборе конкретного облака.
- **Self-hosted Kafka/KRaft:** потенциально дешевле по лицензии, но создаёт on-call и operational risk для команды без SRE.
- **Синхронный REST fan-out:** связывает RTB/callback producer с доступностью всех consumers и не сглаживает пики.

## Последствия

### Положительные

- Statistics, Analytics и Finance масштабируются независимо;
- backpressure измеряется через lag, доступен controlled replay;
- versioned compacted topics поддерживают Bidding projections;
- managed service уменьшает операционную нагрузку первого этапа.

### Отрицательные

- at-least-once требует обязательной дедупликации и reconciliation;
- partitions, retention и quotas требуют capacity/cost-тестов;
- schema registry, DLQ/retry topics и replay runbooks добавляют сложность.

## Критерии проверки

- broker loss при ISR=2 не теряет acknowledged durable events;
- duplicate/out-of-order/replay не нарушают ledger и projections;
- outbox crash points проверены до/после publish и до offset commit;
- throughput 18 000 и 50 000 events/s с реальным P99 message size подтверждён тестами;
- lag critical topics изолирован от analytics/replay quotas.
