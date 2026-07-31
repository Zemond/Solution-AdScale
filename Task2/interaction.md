# Взаимодействие компонентов

## 1. Выбор протоколов


| Взаимодействие                        | Выбор                                                            | Почему                                                                                                                | Почему не альтернатива                                                                          |
| ------------------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| DSP → Gateway                         | HTTPS + JSON OpenRTB 2.x                                         | отраслевой контракт, совместимость DSP, TLS                                                                           | gRPC обычно не является внешним OpenRTB-контрактом; async не позволяет вернуть ставку в аукцион |
| Gateway → Bidding                     | сначала HTTP/JSON pass-through; опционально gRPC после измерений | pass-through минимизирует преобразования и операционный риск; gRPC даёт typed contract/deadline при доказанной пользе | обязательный transcoding усложняет диагностику и может не дать выигрыша                         |
| Management UI → Campaign              | REST/JSON                                                        | CRUD, широкая совместимость, не latency-critical                                                                      | gRPC неудобен браузеру; async не даёт непосредственного результата команды                      |
| Bidding ← Campaign/Finance projection | Kafka compacted topics/CDC + snapshots                           | исключает синхронные зависимости, поддерживает replay и versioning                                                    | REST/gRPC pull на каждый bid нарушает latency и создаёт каскад отказов                          |
| Click/impression → consumers          | Kafka                                                            | durability, buffering, независимые consumers, replay, partition parallelism                                           | REST fan-out связывает producer с доступностью всех consumers; gRPC имеет ту же проблему        |
| Редкие внутренние команды             | gRPC                                                             | schema, deadline propagation, эффективные короткие вызовы                                                             | применять только вне hot path либо при строгом бюджете                                          |


REST, gRPC и Kafka решают разные задачи. Kafka не является способом получить bid response, а gRPC сам по себе не делает вызов надёжным и не отменяет timeout/circuit breaker.