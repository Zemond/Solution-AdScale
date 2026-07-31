# AdScale — данные, масштабирование и отказоустойчивость

Решение задания 3 развивает целевую архитектуру Task1: постепенная декомпозиция по Strangler Fig, разделение RTB- и management-потоков и Kafka для асинхронных событий. Цели: через 3 месяца выдерживать 18 000 RPS без деградации, через год — 50 000 RPS и SLA 99,9%. Для требования DSP по latency RTB-путь проектируется на внешний P95 ≤ 80 ms; внутренний engineering cutoff Bidding — 60 ms (budget allocation, не обещание внутреннего P95).

## Артефакты

- [`database-strategy.md`](database-strategy.md) — хранилища, модели, владение и консистентность.
- [`scaling.md`](scaling.md) — репликация, партиционирование, шардирование, CQRS, RPO/RTO и backup.
- [`caching.md`](caching.md) — ключи Redis, TTL, invalidation, warming, stampede и отказы.
- [`event-streaming.md`](event-streaming.md) — Kafka topics, ключи партиций, схемы, группы, retention, DLQ, outbox и идемпотентность.
- [`diagrams/data-ownership.puml`](diagrams/data-ownership.puml) — владение данными и хранилищами.
- [`diagrams/rtb-data-flow.puml`](diagrams/rtb-data-flow.puml) — критичный RTB-поток и асинхронная фиксация.
- [`diagrams/event-streaming.puml`](diagrams/event-streaming.puml) — producer/topic/consumer-потоки.
- [`diagrams/failover-recovery.puml`](diagrams/failover-recovery.puml) — HA и восстановление данных.
