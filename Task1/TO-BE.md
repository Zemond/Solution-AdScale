# TO-BE — Целевая архитектура AdScale (через 1 год)

## 1. Архитектурный стиль

### Выбор: эволюционная сервисная архитектура по Strangler Fig

**Почему не монолит:**
- Монолит не позволяет горизонтально масштабировать отдельные компоненты
- Критичный Auction Engine зависит от медленных компонентов (Analytics, Stats)
- Гибридный стек (4 языка) в одном процессе — невозможно масштабировать точечно
- Синхронные вызовы к БД в hot path — фундаментальный архитектурный недостаток

**Почему не сразу микросервисы (big bang rewrite):**
- Бюджет ограничен, срок — 3 месяца
- Команда 35 человек без выделенного SRE
- Непрерывность бизнеса — нельзя рисковать полной перепиской
- Высокий риск провала и потери DSP-партнёра

**Почему Strangler Fig Pattern:**
- Позволяет поэтапно выводить компоненты из монолита без простоя
- Возможность тестировать каждый выведенный сервис в продакшене
- Обратимая стратегия — если сервис не работает, можно вернуть в монолит
- Совместимость с ограниченным бюджетом и сроком

## 2. Целевая архитектура (через 1 год)

### 2.1. Обзор

Внешний трафик проходит через API Gateway. Единственный синхронный RTB-путь — `DSP → Gateway → Bidding Service → DSP`. Bidding Service является одним deployable-сервисом; adapter, candidate index, budget guard и ranker — его внутренние компоненты, а не отдельные микросервисы.

Campaign и Finance публикуют версионированные изменения через transactional outbox/CDC. Bidding строит локальную read model и атомарный runtime snapshot. События решений, показов и кликов записываются асинхронно вне зависимости ответа DSP.

### 2.2. Выделенные микросервисы

#### Сервис ставок (Bidding Service) — Go

| Характеристика | Значение |
|---------------|----------|
| **Язык** | Go (высокая производительность, низкая latency) |
| **Роль** | Обработка bid requests, расчёт победителя, бизнес-правила |
| **Данные** | Immutable in-memory L1 snapshot в hot path; Redis L2 и PostgreSQL read model — background warm/recovery |
| **Владение** | Не является источником истины кампаний или денег |
| **Целевая нагрузка** | 18 000 RPS через 3 месяца; 50 000 RPS через год |
| **Целевая задержка** | P95 ответа новой DSP ≤ 80 ms; внутренний engineering cutoff 60 ms |

**Обоснование выделения:** Критичный bottleneck AS-IS архитектуры. Синхронные DB calls в hot path. Требуется изоляция и горизонтальное масштабирование.

#### Сервис выдачи (Delivery Service) — Go

| Характеристика | Значение |
|---------------|----------|
| **Язык** | Go |
| **Роль** | Формирование баннеров, HTTP response |
| **Кэш** | Redis (шаблоны баннеров) |
| **Целевой throughput** | 20 000+ RPS |

**Обоснование:** Высоконагруженный компонент, требует отдельного масштабирования.

#### Сервис статистики (Statistics Service) — Python

| Характеристика | Значение |
|---------------|----------|
| **Язык** | Python |
| **Роль** | Приём событий из Kafka, буферизация, batch write |
| **БД** | PostgreSQL (write-optimized) |
| **Механизм** | Асинхронная приёмка из Kafka, batch write (100-1000 событий) |

**Обоснование:** Нужна буферизация и асинхронная обработка. Kafka решает write bottleneck AS-IS.

#### Сервис финансов (Finance Service) — Go

| Характеристика | Значение |
|---------------|----------|
| **Язык** | Go |
| **Роль** | Резервы, списания, управление балансами, ledger и счета |
| **БД** | PostgreSQL (с transaction isolation) |
| **Особенность** | Finance PostgreSQL — единственный источник истины денег; ACID и double-entry ledger |

**Обоснование:** Финансовые транзакции требуют изоляции и строгой сверки. Kafka переносит интеграционные события, но не определяет баланс.

#### Сервис аналитики (Analytics Service) — Python

| Характеристика | Значение |
|---------------|----------|
| **Язык** | Python |
| **Роль** | Консьюмер Kafka, агрегация данных, чтение из Data Warehouse |
| **БД** | ClickHouse / PostgreSQL (read-optimized) |
| **Особенность** | Чтение только из аналитической БД, не влияет на production-транзакции |

**Обоснование:** Убирает heavy queries из production-БД (проблема AS-IS).

#### Сервис рекламного кабинета (Campaign API) — Go

| Характеристика | Значение |
|---------------|----------|
| **Язык** | Go |
| **Роль** | CRUD кампаний, creatives, targeting и bid settings |
| **БД** | PostgreSQL (CRUD-оптимизированная) |

**Обоснование:** Campaign PostgreSQL — источник истины конфигурации кампаний. Денежные балансы и резервы принадлежат Finance.

#### API Gateway — Envoy Gateway

| Характеристика | Значение |
|---------------|----------|
| **Роль** | Маршрутизация, rate limiting, auth, circuit breaker |
| **Особенность** | Единственная точка входа для DSP, management API и event ingress (callbacks) |

**Обоснование:** Централизованное управление доступом, observability, security.

### 2.3. Инфраструктурные компоненты

| Компонент | Технология | Роль |
|-----------|-----------|------|
| **Event Bus** | Managed Apache Kafka | Интеграционные события, replay и буферизация; не источник истины |
| **Cache** | Managed Redis | Shared L2: versioned projections, warm и recovery; не sync dependency hot path |
| **API Gateway** | Envoy Gateway | Маршрутизация, аутентификация, rate limiting |
| **Container Orchestration** | Kubernetes через год | Оркестрация и масштабирование; не блокирует трёхмесячный этап |
| **CI/CD** | GitHub Actions / GitLab CI | Автоматизированный деплой |
| **Observability** | Prometheus + Grafana + Jaeger | Метрики, трейсы, алертинг |

### 2.4. Базы данных

| Сервис | БД | Тип |
|--------|-----|-----|
| Bidding Service | Immutable L1 snapshot; Redis L2 + PostgreSQL read model вне hot path | Производные конфигурации и ограниченное runtime state |
| Finance Service | HA PostgreSQL | SoT балансов, reservations и ledger |
| Statistics Service | PostgreSQL (write-optimized) | Write-optimized, batch insert |
| Analytics Service | ClickHouse + PostgreSQL | Аналитическая + справочные данные |
| Campaign API | HA PostgreSQL | SoT конфигурации кампаний |
| Региональный кэш | Redis | Производные конфигурации и короткоживущие Finance allowances |


## 3. Этапы миграции (Strangler Fig)

### Фаза 1 (Месяц 1–2) — Подготовка и Foundation

| Задача | Цель |
|--------|------|
| Ввести managed Redis и versioned snapshot | Убрать синхронные management DB reads из RTB hot path |
| Ввести managed Kafka и outbox/CDC foundation | Отделить события и конфигурацию от RTB-ответа |
| Добавить HA managed PostgreSQL/read replica для legacy | Снизить риск единой точки отказа и изолировать аналитику |
| Ввести CI/CD и наблюдаемость | Сделать переключение измеримым и обратимым |

### Фаза 2 (Месяц 2–3) — Промежуточное решение

| Задача | Цель |
|--------|------|
| Выделить Bidding Service (strangler) | Изолировать Auction Engine |
| Ввести gateway/integration layer | Маршрутизация, rate limiting и поэтапное переключение |
| Подключить DSP-партнёра | Выполнить бизнес-требование |
| Load/soak/fault-тесты 18 000 RPS, P95 ≤ 80 ms | Подтвердить промежуточную цель |

### Фаза 3 (Месяц 3–12) — Целевая архитектура

| Задача | Цель |
|--------|------|
| Выделение Delivery, Campaign, Finance и Analytics по приоритету | Постепенно уменьшать монолит |
| Finance Service + double-entry ledger | Изоляция финансовых транзакций |
| Analytics Service + ClickHouse | Убрать heavy queries из production |
| Kubernetes после готовности команды | Оркестрация не блокирует DSP-интеграцию |
| Поддержка 50 000 RPS, SLA 99.9% | Финальная цель |
