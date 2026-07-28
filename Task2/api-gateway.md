# API Gateway для DSP

## 1. Решение

Предпочтительный вариант — **Envoy Gateway** поверх Envoy Proxy, если платформа уже использует Kubernetes и команда готова поддерживать Gateway API. Он даёт быстрый data plane, declarative routing, TLS, rate limiting через extension/service, retries/timeouts, outlier detection и стандартную telemetry.

## 2. Честное разграничение функций

### Gateway отвечает за

- TLS termination и при необходимости mTLS с DSP;
- аутентификацию partner key/JWT/mTLS identity и сопоставление `partner_id`;
- удаление spoofable внутренних headers и добавление доверенного identity context;
- routing по host/path/version/partner;
- per-partner и global rate/concurrency limits;
- body/header size limits, HTTP normalization, защита от malformed transport;
- request timeout, connection pooling, health-based endpoint selection;
- coarse circuit breaking/outlier detection и load shedding;
- access logs, transport metrics, trace context;
- canary/weighted routing для Strangler migration.

### Gateway не отвечает за

- полную бизнес-валидацию OpenRTB objects/extensions;
- eligibility, ranking, pricing, budget/frequency cap;
- хранение idempotency records или финансовые транзакции;
- durable delivery click/impression;
- «гарантию SLA» без capacity, application и network engineering;
- глубокий payload logging, содержащий PII.

Bidding Service остаётся владельцем OpenRTB semantics. WAF полезен на публичной границе, но его rules должны быть протестированы на больших валидных bid payloads, чтобы не создавать latency/false positives.

## 3. Маршруты

| Route | Клиент | Upstream | Политика |
|---|---|---|---|
| `POST /openrtb/2.x/bid` | DSP | Bidding Service | auth, partner quota, body limit, 60 ms upstream cutoff, retries off |
| `GET /health/partner` | DSP/NOC при необходимости | статический/gateway handler | без проверки транзитивных зависимостей |
| `/callbacks/impression/{token}` | browser/exchange | Event Collector | signed token, dedup, async Kafka |
| `/callbacks/click/{token}` | browser/exchange | Event Collector/redirector | signed token, allowlisted redirect, dedup, async Kafka |
| `/metrics`, `/readyz`, admin | internal only | gateway/service | отдельная сеть и auth; не публиковать DSP |

Callback ingestion отделяется от Bidding по pool/route и не конкурирует с аукционами. Открытый redirect в click endpoint запрещён: destination подписывается или берётся по opaque creative token.

## 4. Аутентификация и безопасность

- Предпочтительно mTLS для стабильного B2B DSP; fallback — короткоживущий signed JWT или API key через TLS.
- Secrets хранятся в secret manager, имеют `kid`, период ротации и overlap старого/нового ключа.
- Source IP allowlist — дополнительный сигнал, не единственный механизм.
- Gateway нормализует path/method/content-type, ограничивает body, headers и время чтения slow client.
- Внутренний hop защищён mTLS или приватной сетью с workload identity.
- Access log содержит partner, route, status, duration, bytes, outcome reason class; payload, user identifiers и credentials редактируются.
- Подпись callback token включает `bid_id`, event type, expiry и key id; проверка constant-time, replay обрабатывается идемпотентно.

## 5. Rate limiting и overload control

Два уровня:

1. **Per-partner token bucket** — contracted sustained RPS + burst.
2. **Global concurrency/admission limit** — защищает измеренную safe capacity Bidding.

Локальный limiter на каждом gateway недостаточен для строгой общей квоты при нескольких replicas. Возможны:

- Envoy external rate limit service с Redis — точнее, но добавляет зависимость;
- приблизительные локальные лимиты с делением квоты по replicas — проще и остаются работоспособны при отказе Redis;
- managed gateway limiter — если подтверждены semantics и стоимость.

Для RTB предпочтителен fail-open только в пределах локального safety cap: при отказе distributed limiter gateway не пропускает неограниченный поток. При saturation excess получает ранний no-bid/`429` согласно partner profile.

Квоты выводятся из контракта и capacity test. Значения «5 000 RPS на pod» без измерений не задаются.

## 6. Timeout, retry и circuit breaking

### Timeout

- request header/body read timeout защищает от slow clients;
- gateway вычисляет абсолютный deadline из partner profile и `tmax`;
- upstream cutoff — проектно 60 ms, но может быть меньше;
- idle/connection timeout не смешивается с request timeout;
- keep-alive и connection pool уменьшают handshake cost.

### Retry

Для `POST /openrtb/2.x/bid` retries **выключены по умолчанию**. Envoy не должен автоматически повторять запрос при reset/5xx: остатка 80 ms мало, а повтор может вызвать вторичную обработку. Однократный retry/hedge допустим только после отдельного теста, с replay cache/idempotency contract и строгим retry budget.

Retries допустимы для idempotent control-plane GET и вне hot path — с exponential backoff, jitter и лимитом.

### Circuit breaker и outlier detection

- Ограничить pending requests, active connections/requests и retries.
- Исключать endpoint по consecutive gateway/connect failures и статистическому outlier detection.
- Half-open probes должны быть ограничены.
- Если healthy capacity недостаточна, не перенаправлять бесконечно на оставшиеся replicas: admission control возвращает no-bid.
- Пороговые значения настраиваются по baseline и fault tests; слишком агрессивный breaker сам вызывает outage.

Gateway breaker защищает сетевой upstream. Доменные fallback/stale snapshot остаются ответственностью Bidding.

## 7. Маршрутизация и rollout

- Partner-specific listener/virtual host либо metadata policy отделяет auth, quota и OpenRTB profile.
- Weighted route направляет сначала shadow traffic (ответ игнорируется), затем 1%/5%/25%/50%/100% в новый Bidding.
- Shadow не публикует финансовые/callback события и маркируется `dry_run`, иначе создаст дубли.
- Promotion guardrails: external latency, gateway 5xx, technical no-bid, snapshot freshness и saturation.
- Rollback меняет route без redeploy Bidding. Старый монолит используется как rollback только если его профиль укладывается в партнёрский дедлайн; иначе безопасный rollback — no-bid, а не поздний ответ.

## 8. Высокая доступность и масштабирование

### Этап три месяца

- минимум две gateway replicas в разных failure domains за L4 load balancer;
- PodDisruptionBudget/anti-affinity, rolling update с connection drain;
- заранее выделенная capacity для отказа одной replica/зоны;
- конфигурация в Git, schema validation и canary перед rollout;
- один регион, потому что преждевременный multi-region усложнит эксплуатацию и не требуется для подключения за три месяца.

### Горизонт год

- regional gateway + Bidding stacks, GeoDNS/Anycast по доступным возможностям;
- routing в локальный регион, без межрегионального вызова в bid hot path;
- partner credentials/config распространяются versioned control plane;
- независимое измерение regional SLO и controlled failover. Failover capacity проверяется тестом, а не предполагается.

## 9. Monitoring и SLO

Gateway экспортирует:

- RPS, active/pending requests, connection/TLS errors;
- latency histogram end-to-end и upstream отдельно по partner/route;
- response status и нормализованный no-bid reason;
- rate-limit decisions, circuit state, ejected endpoints;
- request/response bytes и body rejection;
- config version и rollout cohort.
