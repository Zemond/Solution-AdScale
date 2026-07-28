# Стратегия кэширования

## 1. Цель и границы

RTB hot path читает **только immutable in-memory snapshot / L1**. Это нужно, чтобы удержать внешний P95 ≤ 80 ms при 18k RPS (внутренний engineering cutoff 60 ms) и масштабироваться до 50k RPS.

Redis — **shared L2** для распространения versioned проекций, прогрева и восстановления между replicas. PostgreSQL read-модель Bidding — durable projection и источник rebuild. Ни Redis, ни PostgreSQL не являются синхронными зависимостями bid response.

Кэш не является источником истины. Потеря Redis не должна приводить к потере кампаний, проводок или денег.

Финансовый баланс и итоговое списание определяет только Finance PostgreSQL. В Redis / L1 допускаются:

- короткоживущий `spendable allowance`/reservation token, выданный Finance;
- display-only snapshot баланса с `as_of` (только UI, вне RTB);
- rate counter для soft guard.

Они могут запретить показ (fail closed), но не разрешить расход сверх подтверждённого Finance лимита.

## 2. Размещение

- Региональный Redis рядом с Bidding в private subnet; cross-region доступ в RTB запрещён.
- Для 3 месяцев — managed Redis primary + replica и automatic failover либо Sentinel; на 50k RPS — Redis Cluster после capacity test.
- Bidding pod держит L1: атомарный immutable versioned snapshot. Redis — shared L2 для projector/loader. L1 ограничен размером и очищается событием/TTL/`max_snapshot_age`.
- Campaign/Finance пишут только в свои PostgreSQL и outbox. Отдельный cache projector применяет события к Redis L2 и инициирует L1 refresh; Bidding не получает право записи в owned objects.

## 3. Ключи, TTL и поведение

Все ключи содержат environment и schema version: `adscale:{env}:v1:...`. Для Redis Cluster hash tag используется только когда несколько ключей действительно требуют атомарной операции.

| Данные | Ключ | L1 | Redis L2 TTL | Инвалидация / background fallback |
|---|---|---|---:|---|
| Campaign eligibility/status | `campaign:{id}:eligibility:v{version}` | в immutable snapshot | 60 s + jitter 0–10 s | Event update/stop удаляет старую версию и обновляет pointer; miss в L2 → Bidding read-model PostgreSQL **вне hot path** |
| Targeting rules | `campaign:{id}:targeting:v{version}` | в immutable snapshot | 5 min + jitter | Versioned replacement; rebuild из read-model вне hot path |
| Bid configuration/max bid | `campaign:{id}:bid-config:v{version}` | в immutable snapshot | 30 s + jitter | Event invalidation; stale max bid не используется после stop/budget revoke |
| Creative metadata/template ref | `creative:{id}:v{version}` | в immutable snapshot | 30 min | Event invalidation; binary asset — CDN/object storage |
| Active campaign index by segment | `segment:{segment}:campaigns:v{generation}` | в immutable snapshot | 30 s | Rebuild generation, atomic pointer swap |
| Finance allowance token | `allowance:{account}:{campaign}` | в immutable snapshot | До срока в signed token, максимум 5–15 s | Только Finance выпускает/возобновляет; miss/expired в L1 → no-bid; renew allowance вне hot path |
| UI balance snapshot | `balance-display:{account}` | нет | 5 s | Event update; маркируется `as_of`, не используется для charge / RTB |
| Negative result | `campaign:{id}:missing` | краткий marker в snapshot generation | 5 s | Снимается create/update event; защищает background loader от cache penetration |

TTL задаёт верхнюю границу жизни при потерянной invalidation, но не заменяет событийную инвалидацию. Security stop, campaign pause и revoked allowance публикуются high-priority событиями; Bidding применяет monotonic `version` и игнорирует старые сообщения.

## 4. Паттерны доступа

### Hot path: только L1

1. Bidding читает текущий immutable snapshot pointer в памяти.
2. Eligibility, targeting, bid config и allowance берутся только из этого snapshot.
3. При отсутствии данных, stale `max_snapshot_age`, неизвестной/истёкшей/исчерпанной allowance — **no-bid**.
4. В request goroutine **нет** `GET` в Redis и **нет** SQL к PostgreSQL.

### Background: L2 warm / recovery / refresh

1. Projector/loader обновляет Redis L2 после commit локальной read-модели.
2. Pod загружает/обновляет L1 из Redis L2 или из PostgreSQL read-модели при старте, после invalidation и по schedule.
3. На L2 miss loader читает Bidding PostgreSQL read-модель (не Campaign DB), пишет `SET ... NX EX`/version check с TTL+jitter.
4. Если read-модель отсутствует или слишком стара, L1 не переключается на небезопасную версию; readiness/no-bid защищают трафик.

### Event-driven invalidation

Campaign commit + outbox → `campaign.configuration.v1` → Bidding projector:

1. в одной транзакции read-модели проверяет event inbox и version;
2. обновляет PostgreSQL projection;
3. после commit обновляет versioned Redis L2 value и pointer;
4. публикует локальный invalidation / reload для L1.

Удаление — tombstone с версией, а не незащищённый `DEL`: запоздавшее старое событие не должно воскресить кампанию.

### Write-through не используется для SoT

Приложение не подтверждает Campaign/Finance write по записи в Redis. Сначала commit PostgreSQL, затем асинхронное обновление кэша. Это исключает признание кэша источником истины.

## 5. Прогрев

- Перед переключением DSP-трафика Campaign exporter формирует versioned snapshot активных кампаний в Bidding read-модель.
- Warm-up job загружает top-N активных кампаний/сегментов по данным предыдущих 24 часов с ограничением RPS и memory budget в Redis L2, затем в L1.
- Новые Bidding pods при readiness загружают небольшой immutable L1 snapshot; readiness не требует загрузки всего каталога и не делает sync Redis/PostgreSQL на каждый bid.
- После failover Redis прогрев идёт постепенно: hot keys first, bounded concurrency, rate limit к PostgreSQL, наблюдение за L2 hit ratio и L1 freshness.
- Blue/green release использует новый namespace/generation, проверяет counts/checksum, затем атомарно меняет pointer; старое поколение удаляется позже.

Цель после прогрева: hit ratio Redis L2 ≥ 95% для campaign config на background load path и L1 coverage ≥ 99% на RTB hot set. Порог подтверждается измерением, а не считается гарантией.

## 6. Защита от stampede и hot keys

- TTL jitter ±10–20%, чтобы ключи не истекали одновременно.
- Request coalescing/singleflight на pod **для background loader**: один loader на ключ, остальные ждут bounded time.
- Distributed lock только для дорогого rebuild: `SET lock token NX PX`; release через compare-and-delete Lua. Lock timeout короче loader timeout.
- Soft TTL + hard TTL на L2: один worker refresh-ahead после soft expiry. Pause/revoke/allowance никогда не обслуживаются stale в L1.
- Negative caching и Bloom filter для несуществующих IDs при доказанной атаке/нагрузке (background path).
- Hot segment lists разбиваются по generation/bucket, реплицируются в L1; запрещён один глобальный mutable sorted set.

## 7. Отказы и деградация

| Отказ | Поведение |
|---|---|
| Redis timeout / недоступен | Hot path продолжает на текущем L1. Background loader открывает circuit; rebuild из PostgreSQL read-модели с rate limit. Нет безопасного L1/allowance → no-bid / readiness=false |
| Cache projector lag | Alert при >5 s; возраст/версия проверяются перед atomic swap L1. Новые/изменённые кампании fail closed до применения |
| PostgreSQL read-model недоступна | Используется только неистёкший L1; L2 rebuild откладывается. По hard TTL / `max_snapshot_age` — no-bid |
| Finance недоступен | Уже выданные, неистёкшие allowance tokens в L1 действуют в их лимите; новые не выдаются. Никаких списаний «по Redis» |
| Redis failover | Reconnect/topology refresh только в background loader; bid request не ждёт failover |

Чтобы cache failure не превратился в DB outage, loader имеет semaphore/bulkhead и глобальный rate limit; при превышении возвращается no-bid на уровне readiness/snapshot age, а не sync fallback из аукциона.

## 8. Согласованность бюджетов

Для high-RPS нельзя синхронно запрашивать Finance на каждый auction и нельзя считать деньги Redis-счётчиком. Используется escrow/reservation:

1. Finance ACID-транзакцией резервирует ограниченный allowance для `(account_id, campaign_id, epoch)` и записывает ledger/reservation + outbox → `finance.allowance.v1`.
2. Bidding materializes лимит в L1 (через L2 projector) и атомарно расходует только выделенную квоту **в памяти** в рамках snapshot epoch.
3. Фактические impressions/clicks поступают в Finance идемпотентно; Finance проводит charge и сверяет reservation.
4. Неиспользованный allowance истекает/возвращается; reconciliation выявляет расхождения.
5. Сумма одновременно выданных allowances не превышает доступный баланс по Finance PostgreSQL.

Таким образом, потеря Redis может вызвать no-bid или повторную сверку, но не создаёт новый денежный баланс.

## 9. Наблюдаемость и тесты

- Метрики: L1 coverage/age/version, L2 hit/miss по namespace, p50/p95/p99 loader latency, memory fragmentation, evictions, rejected connections, failover duration, hot keys, loader RPS, singleflight wait, stale age.
- Alerts: L1 `max_snapshot_age` breach, L2 hit ratio <90% 10 минут, evictions >0, memory >75%, loader p99 > budget, projector lag >5 s, replication link down.
- Тесты: cold L1/L2 при 18k/50k RPS; одновременное истечение 10% ключей; Redis node loss без sync fallback в hot path; lost/reordered invalidation; stale version; Finance outage; memory pressure.
