# Bidding Service

## 1. Назначение и границы

Bidding Service — stateless с точки зрения сессии RTB, независимо масштабируемый сервис на Go. Он принимает нормализованный OpenRTB bid request, за ограниченное время выбирает допустимый creative и цену и возвращает bid response либо no-bid.

### В зоне ответственности

- синтаксическая и семантическая валидация поддерживаемого профиля OpenRTB 2.x;
- нормализация partner-specific extensions через версионированный adapter;
- eligibility и targeting по placement/device/geo/audience/context;
- frequency-cap и budget guard по локальной, консервативной проекции;
- расчёт цены, ранжирование и детерминированный tie-break;
- формирование `seatbid`, `bid`, `adm`/creative reference, `nurl`/`burl`/`lurl` согласно контракту партнёра;
- формирование decision event без ожидания его внешней доставки;
- метрики, структурированные логи без PII и распределённый trace sampling.

### Вне зоны ответственности

- CRUD кампаний и креативов — Campaign Service/существующий монолит;
- авторитетный баланс, списания и ledger — Finance Service;
- рендеринг/хостинг ассетов — Delivery/Creative Service;
- долговременная статистика и аналитика — consumers Kafka;
- rate limiting, TLS, проверка partner credentials — API Gateway;
- межпартнёрская оркестрация и закупка трафика — не входят в этот сервис.

Такое разделение не допускает management/finance/analytics traffic в RTB hot path.

## 2. Внешний и внутренний API

### 2.1. Внешний контракт через gateway

`POST /openrtb/2.x/bid`

Gateway не разбирает бизнес-смысл OpenRTB и не выбирает ставку.

### 2.2. Внутренний gRPC

Gateway может передавать HTTP/JSON без транскодирования, что проще для первого этапа. 

### 2.3. Management endpoints

- `GET /livez` — процесс жив; не проверяет Kafka/Redis/PostgreSQL.
- `GET /readyz` — экземпляр принимает трафик только при загруженном snapshot допустимого возраста.
- `GET /metrics` — Prometheus; закрыт от внешней сети.
- административная перезагрузка snapshot — только через защищённый control plane, не через DSP route.

## 3. Зависимости

| Зависимость | Контракт | В hot path | Поведение при отказе |
|---|---|---:|---|
| API Gateway | HTTPS/JSON или gRPC | да | gateway возвращает no-bid/ошибку согласно partner profile |
| Campaign projection | Kafka compacted topic/CDC + periodic snapshot | нет | использовать последний snapshot до `max_snapshot_age`, затем readiness=false/no-bid |
| Budget projection | Kafka compacted topic/CDC | нет | консервативный локальный лимит; при stale critical budget data — no-bid |
| Redis | восстановление snapshot/общие короткоживущие данные | нет для обычного bid | circuit open; продолжить на локальном snapshot |
| Kafka event ingress | async produce из spool | нет | bounded buffering, алерт, controlled shedding; не увеличивать latency ответа |
| Delivery/Creative | заранее опубликованные URL/templates в snapshot | нет | creative без готового артефакта не eligible |
| PostgreSQL | авторитетные данные management plane | нет | Bidding не обращается к БД на каждый запрос |

Отказ Redis/Kafka/PostgreSQL не должен создавать каскад синхронных retries из аукциона.

## 4. Модель данных

Авторитетные записи принадлежат Campaign и Finance. Bidding хранит read model, оптимизированную под выборку. Для трёхмесячного этапа проекция может физически лежать в отдельной schema/БД и Redis, но владение и путь миграции остаются явными.


## 5. Алгоритм и деградация

1. Проверить размер, профиль, обязательные поля и остаток deadline.
2. Прочитать один указатель на текущий immutable snapshot.
3. Отфильтровать кандидатов по supply policy, campaign/creative state, targeting и floor.
4. Применить caps и budget tokens. Если состояние, обязательное для финансовой безопасности, устарело — исключить кандидата.
5. Рассчитать цену и выбрать победителя детерминированно.
6. Сформировать response; поставить decision telemetry в bounded spool без блокирующего ожидания.
7. Если шаг не успевает завершиться до внутреннего cutoff — отменить вычисление и вернуть no-bid.

Fallback — это последний валидный snapshot и заранее проверенные creatives. «Дефолтная ставка» без актуального eligibility/budget запрещена: она может создать финансовый и compliance-риск.

