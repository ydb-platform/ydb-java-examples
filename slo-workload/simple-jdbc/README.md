# Simple JDBC SLO Test

Базовый SLO тест для YDB JDBC Driver без использования фреймворков (plain JDBC).

## Описание

Тестирует производительность и надежность YDB JDBC Driver под нагрузкой:
- **Latency**: P50 < 10ms, P95 < 50ms, P99 < 100ms
- **Success Rate**: > 99.9%
- **Технологии**: YDB JDBC Driver, Spring Boot (только для DI)

> 📖 **Подробнее о стратегии тестирования, метриках и архитектуре:** [slo-workload/README.md](../README.md)

## Быстрый старт

### Запуск в CI/CD

Тест автоматически запускается через GitHub Actions при изменениях в `slo-workload/**`.

**Workflow:** `.github/workflows/slo-test.yml`

Пропустить тест: добавьте label `no slo` к PR.

### Параметры

| Параметр | Описание | Значение по умолчанию |
|----------|----------|----------------------|
| `TEST_DURATION` | Длительность теста (секунды) | 60 |
| `READ_RPS` | Read операций в секунду | 1000 |
| `WRITE_RPS` | Write операций в секунду | 100 |
| `READ_TIMEOUT` | Timeout для read (ms) | 1000 |
| `WRITE_TIMEOUT` | Timeout для write (ms) | 1000 |

## Компоненты

### JdbcSloTableContext
Сервисный класс для работы с таблицей `slo_table`.

**Основные методы:**
```java
createTable(timeout)           // Создание таблицы
save(row, timeout)             // UPSERT с retry
select(guid, id, timeout)      // SELECT с retry
```

**Особенности:**
- Retry с exponential backoff (5 попыток)
- Автоматическое восстановление после временных ошибок
- Подробности: см. [родительский README](../README.md#retry-механизм)

### SloTableRow
DTO для строки таблицы (~1KB payload).
```java
SloTableRow row = SloTableRow.generate(id);
// Поля: guid, id, payloadStr, payloadDouble, payloadTimestamp
```

### JdbcSloTest
Основной тест JUnit, выполняющий полный цикл SLO тестирования.

**Фазы:**
1. Инициализация таблицы
2. Warmup (10 потоков, 10s)
3. Load test (30 потоков, TEST_DURATION)
4. Валидация SLO
5. Экспорт метрик

### MetricsReporter
Экспорт метрик в Prometheus Push Gateway.

**Метрики:**
- `sdk_operations_total` - всего операций
- `sdk_operations_success_total` - успешных операций
- `sdk_operation_latency_seconds` - histogram latency
- `sdk_pending_operations` - активных операций

Подробнее о метриках: [родительский README](../README.md#метрики)

## Локальная разработка

Разработка кода возможна локально, но **полноценный запуск теста только в CI/CD** (требуется YDB):
```bash
# Компиляция
mvn clean compile -pl slo-workload/simple-jdbc

# Тесты (требует YDB)
mvn test -pl slo-workload/simple-jdbc -Dskip.jdbc.tests=false
```

## Troubleshooting

### JDBC-специфичные проблемы

**Connection pool exhausted**
```
Симптом: Много ошибок "Cannot get connection from pool"
Решение: Увеличить pool size в SimpleJdbcConfig
```

**Prepared statement cache issues**
```
Симптом: OutOfMemoryError: Metaspace
Решение: Ограничить кэш prepared statements
```

**Long GC pauses**
```
Симптом: Периодические всплески latency P99
Решение: Настроить JVM параметры (-XX:+UseG1GC)
```

### Общие проблемы

Для общих проблем (compilation, YDB connection, метрики) см. [Troubleshooting в родительском README](../README.md#troubleshooting).

## Развитие

### Сравнение с другими реализациями

После появления других workload'ов (Spring JdbcTemplate, Spring Data JDBC, JPA) можно будет сравнить:
- Overhead каждого слоя абстракции
- Trade-off между производительностью и удобством
- Рекомендации по выбору стека

### Оптимизации

Потенциальные улучшения этого теста:
- [ ] Batch operations для write
- [ ] Использование prepared statements
- [ ] Connection pool tuning
- [ ] Асинхронное логирование

## Ссылки

- [Родительский README (стратегия, метрики, архитектура)](../README.md)
- [YDB JDBC Driver](https://github.com/ydb-platform/ydb-jdbc-driver)
- [YDB Documentation](https://ydb.tech/docs/)