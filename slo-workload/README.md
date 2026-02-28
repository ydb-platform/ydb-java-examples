# YDB SLO Workload Tests

Набор SLO (Service Level Objectives) тестов для проверки производительности и надежности YDB клиентов на Java.

## Назначение

SLO тесты измеряют соответствие реальной производительности заявленным целевым метрикам:
- **Latency**: время отклика операций (P50, P95, P99)
- **Availability**: процент успешных операций
- **Throughput**: количество операций в секунду
- **Stability**: стабильность под длительной нагрузкой

## Стратегия тестирования

### Фазы выполнения

#### 1. Инициализация (Setup Phase)
```
Действия:
- Создание таблицы slo_table
- Проверка подключения к YDB
- Инициализация метрик

Длительность: ~5 секунд
```

#### 2. Прогрев (Warmup Phase)
```
Зачем нужен:
- JIT компиляция "горячих" методов
- Инициализация connection pool
- Стабилизация JVM (GC, memory allocation)
- Прогрев кэшей YDB

Параметры:
- Потоки: 10 (read: 8, write: 2)
- Длительность: 10 секунд
- RPS: 50% от целевой нагрузки

Критерий готовности:
- Latency P95 стабилизировалась (не растет)
- Success rate > 95%
```

**Почему 10 потоков?**
- Достаточно для JIT компиляции критичных методов
- Не создает избыточную нагрузку на YDB
- Позволяет обнаружить проблемы до основного теста

#### 3. Рабочая нагрузка (Load Phase)
```
Параметры:
- Потоки: 30 (read: 24, write: 6)
- Длительность: TEST_DURATION секунд (default: 60)
- Read RPS: READ_RPS (default: 1000)
- Write RPS: WRITE_RPS (default: 100)

Соотношение read/write = 10:1 (типичное для OLTP)
```

**Почему 30 потоков?**
- Имитирует реальную многопользовательскую нагрузку
- Достаточно для насыщения connection pool
- Выявляет проблемы конкурентного доступа
- Соответствует типичной нагрузке веб-приложений (20-50 одновременных запросов)
- **Рассчитано на кластер из 5 нод** (текущее значение `ydb_database_node_count`)

**Распределение потоков (24 read / 6 write):**
- Отражает реальное соотношение операций в OLTP системах
- Read-heavy workload типичен для большинства приложений
- Write потоки создают достаточную конкуренцию за ресурсы

> ⚠️ **Важно:** Количество потоков следует масштабировать в зависимости от размера кластера:
> - **3 ноды:** 18-20 потоков (6 потоков на ноду)
> - **5 нод:** 30 потоков (6 потоков на ноду) ← текущая конфигурация
> - **10 нод:** 50-60 потоков (5-6 потоков на ноду)
>
> Эмпирическое правило: **~6 потоков на ноду** обеспечивает оптимальную утилизацию без перегрузки.
> В будущих версиях планируется автоматический расчет количества потоков на основе `ydb_database_node_count`.

#### 4. Валидация результатов (Validation Phase)
```
Проверяемые SLO:
✓ P50 latency < 10 ms
✓ P95 latency < 50 ms  
✓ P99 latency < 100 ms
✓ Success rate > 99.9%

Если хотя бы один порог не пройден → тест FAILED
```

#### 5. Экспорт метрик (Export Phase)
```
Действия:
- Отправка метрик в Prometheus Push Gateway
- Сохранение результатов в файл
- Генерация отчета для GitHub
```

## Метрики

### Основные метрики Prometheus
```
sdk_operations_total{operation_type, sdk, sdk_version}
  Counter - общее количество операций
  Labels: read, write, upsert

sdk_operations_success_total{operation_type, sdk, sdk_version}
  Counter - успешные операции
  Используется для расчета Success Rate

sdk_operation_latency_seconds{operation_type, operation_status}
  Histogram - распределение latency
  Buckets: 1ms, 2ms, 3ms, 4ms, 5ms, 7.5ms, 10ms, 20ms, 50ms, 100ms, 200ms, 500ms, 1s
  Позволяет вычислить P50, P95, P99

sdk_pending_operations{operation_type}
  Gauge - количество операций в процессе выполнения
  Индикатор перегрузки системы
```

### Вычисляемые метрики

**Success Rate:**
```
success_rate = (sdk_operations_success_total / sdk_operations_total) * 100%
```

**Error Rate:**
```
error_rate = 100% - success_rate
```

**Throughput (RPS):**
```
actual_rps = sdk_operations_total / test_duration_seconds
```

**Percentiles (P50, P95, P99):**
```
Вычисляются из histogram buckets в Prometheus/Grafana
```

## Retry механизм

Все SLO тесты используют единую стратегию retry:
```
Стратегия: Exponential Backoff
Максимум попыток: 5
Backoff delays: 100ms → 200ms → 400ms → 800ms → 1600ms

Повторяемые ошибки (transient):
- Connection timeout
- Network errors  
- Session expired
- Overload (503)
- Unavailable (503)

Не повторяемые ошибки (permanent):
- Schema errors (table not found, column mismatch)
- Constraint violations (unique, foreign key)
- Syntax errors
- Permission denied
```

**Почему именно такая стратегия?**
- Exponential backoff снижает нагрузку на перегруженную систему
- 5 попыток достаточно для восстановления после кратковременных сбоев
- Максимальная задержка 1.6s не блокирует тест надолго

## Структура проекта
```
slo-workload/
├── pom.xml                    # Родительский POM (Java 21, зависимости)
├── README.md                  # Этот файл
│
├── simple-jdbc/               # SLO тест для базового JDBC
│   ├── src/
│   │   ├── main/java/tech/ydb/slo/
│   │   │   ├── JdbcSloTableContext.java    # Управление таблицей
│   │   │   ├── MetricsReporter.java        # Экспорт метрик
│   │   │   ├── SloTableRow.java            # DTO
│   │   │   └── SimpleJdbcConfig.java       # Spring конфиг
│   │   └── test/java/tech/ydb/slo/
│   │       └── JdbcSloTest.java            # Основной тест
│   ├── pom.xml
│   └── README.md
│
└── [future workloads]/
    ├── spring-jdbc/           # Spring JdbcTemplate
    ├── spring-data-jdbc/      # Spring Data JDBC  
    └── spring-data-jpa/       # Spring Data JPA
```

## Текущие реализации

### ✅ simple-jdbc
Базовый JDBC драйвер без фреймворков.

**Статус:** Готов  
**Технологии:** YDB JDBC Driver, Spring Boot (только DI)  
**Документация:** [simple-jdbc/README.md](simple-jdbc/README.md)

## Планируемые реализации

### 🔄 spring-jdbc
Использование Spring JdbcTemplate.

**Цель:** Проверить overhead Spring JdbcTemplate  
**Ожидаемый результат:** Latency +2-3ms по сравнению с plain JDBC

### 🔄 spring-data-jdbc
Spring Data JDBC (без JPA).

**Цель:** Измерить производительность Spring Data абстракции  
**Ожидаемый результат:** Latency +5-10ms, упрощение кода

### 🔄 spring-data-jpa
Полноценный JPA/Hibernate stack.

**Цель:** Оценить overhead JPA  
**Ожидаемый результат:** Latency +10-20ms, максимальное удобство разработки

### 🔄 webflux-r2dbc
Реактивный подход (WebFlux + R2DBC).

**Цель:** Сравнить reactive vs blocking под высокой нагрузкой  
**Ожидаемый результат:** Лучшая throughput при >100 одновременных запросах

## Планируемые улучшения

### Инфраструктура

#### 🔄 Динамическое масштабирование потоков
**Проблема:** Фиксированное количество потоков (30) не оптимально для кластеров разного размера.

**Решение:** Автоматический расчет на основе `ydb_database_node_count`:
```java
int optimalThreads = ydbNodeCount * THREADS_PER_NODE; // 6 потоков на ноду
int readThreads = (int) (optimalThreads * 0.8);       // 80% read
int writeThreads = optimalThreads - readThreads;      // 20% write
```

**Преимущества:**
- Оптимальная утилизация кластера любого размера
- Предсказуемая нагрузка на каждую ноду
- Лучшее распределение запросов

**Статус:** Планируется после стабилизации текущих тестов

#### 🔄 Адаптивная стратегия warmup
Длительность прогрева также зависит от размера кластера:
- 3 ноды: 5-7 секунд
- 5 нод: 10 секунд (текущее)
- 10+ нод: 15-20 секунд

#### 🔄 Тесты масштабируемости
Автоматическое тестирование на кластерах разного размера:
```yaml
matrix:
  ydb_nodes: [3, 5, 10]
  # Автоматически: threads = nodes * 6
```

**Ожидаемые результаты:**
- Linear scaling throughput: 3 ноды → 5 нод = +67% RPS
- Latency остается стабильной при правильном масштабировании потоков

### Тестовые сценарии

#### 🔄 Chaos Testing
- Рестарты нод во время теста
- Network partitions
- Увеличение latency сети

#### 🔄 Soak Tests
- Длительные тесты (24h+)
- Детекция memory leaks
- Проверка стабильности под постоянной нагрузкой

#### 🔄 Различные размеры payload
- Small (100 bytes)
- Medium (1 KB) ← текущий
- Large (10 KB)
- Extra Large (100 KB)

## Использование в CI/CD

### В ydb-java-examples (текущий репозиторий)

Workflow: `.github/workflows/slo-test.yml`
```yaml
Триггеры:
- Push в master/main
- Pull Request с изменениями в slo-workload/

Что делает:
1. Поднимает YDB через ydb-slo-action
2. Собирает проект (Maven)
3. Запускает SLO тесты
4. Публикует графики в PR
```

### В ydb-jdbc-driver / ydb-java-sdk (будущее)

Workflow: `.github/workflows/slo.yml`
```yaml
Что будет делать:
1. Checkout текущего SDK/JDBC драйвера
2. Checkout ydb-java-examples
3. Собрать текущую версию драйвера
4. Обновить версию в SLO проекте
5. Запустить SLO тесты с новой версией
6. Публиковать результаты

Цель:
- Проверять каждый PR на регрессию производительности
- Блокировать merge при нарушении SLO
```

## Добавление нового workload

### Шаг 1: Создайте модуль
```bash
cd slo-workload
cp -r simple-jdbc your-workload
cd your-workload
```

### Шаг 2: Модифицируйте код
```java
// YourSloTest.java
@SpringBootTest
public class YourSloTest {
    
    @Test
    public void runSloTest() {
        // 1. Setup
        // 2. Warmup (10 threads, 10s)
        // 3. Load test (30 threads, TEST_DURATION)
        // 4. Validate SLO
        // 5. Export metrics
    }
}
```

### Шаг 3: Обновите pom.xml
```xml
<!-- slo-workload/pom.xml -->
<modules>
    <module>simple-jdbc</module>
    <module>your-workload</module>
</modules>
```

### Шаг 4: Обновите workflow
```yaml
# .github/workflows/slo-test.yml
matrix:
  workload:
    - simple-jdbc
    - your-workload
```

### Шаг 5: Документация

Создайте `your-workload/README.md` с описанием специфики вашего теста.

## Интерпретация результатов

### ✅ Успешный тест
```
✓ P50: 5.2 ms  (порог: 10 ms)
✓ P95: 23.1 ms (порог: 50 ms)
✓ P99: 45.8 ms (порог: 100 ms)
✓ Success Rate: 99.95% (порог: 99.9%)
```

**Интерпретация:** Производительность в норме, можно мержить PR.

### ⚠️ Warning: близко к порогу
```
✓ P50: 8.7 ms  (порог: 10 ms) ⚠️
✓ P95: 47.3 ms (порог: 50 ms) ⚠️
✓ P99: 89.2 ms (порог: 100 ms)
✓ Success Rate: 99.91% (порог: 99.9%)
```

**Интерпретация:** Тест прошел, но метрики близки к порогам. Рекомендуется:
- Проверить код на возможные оптимизации
- Запустить тест повторно для подтверждения
- Рассмотреть профилирование

### ❌ Failed: нарушение SLO
```
✓ P50: 12.4 ms  (порог: 10 ms) ❌
✓ P95: 78.9 ms  (порог: 50 ms) ❌
✓ P99: 156.3 ms (порог: 100 ms) ❌
✓ Success Rate: 99.45% (порог: 99.9%) ❌
```

**Интерпретация:** Регрессия производительности. Действия:
1. Сравнить с предыдущим успешным прогоном
2. Найти изменения, которые могли повлиять
3. Профилировать код (JProfiler, async-profiler)
4. Проверить конфигурацию (connection pool, timeouts)

## Best Practices

### При разработке нового workload

✅ **DO:**
- Используйте единую стратегию прогрева (10 потоков, 10с)
- Следуйте паттерну 30 потоков для основной нагрузки (на 5 нод)
- Реализуйте retry с exponential backoff
- Экспортируйте стандартные метрики
- Документируйте специфику вашего теста

❌ **DON'T:**
- Не меняйте SLO пороги без обоснования
- Не пропускайте фазу warmup
- Не используйте фиксированные задержки вместо exponential backoff
- Не игнорируйте ошибки (всегда логируйте и считайте)

### При настройке нагрузки

✅ **DO:**
- Учитывайте размер кластера при выборе количества потоков
- Используйте формулу: `threads ≈ nodes * 6` как отправную точку
- Мониторьте утилизацию каждой ноды (должна быть равномерной)
- Начинайте с меньшей нагрузки и постепенно увеличивайте

❌ **DON'T:**
- Не используйте 30 потоков для кластера из 3 нод (перегрузка)
- Не используйте 30 потоков для кластера из 20 нод (недогрузка)
- Не сравнивайте результаты тестов на кластерах разного размера
- Не запускайте 100+ потоков без обоснования

### При анализе результатов

✅ **DO:**
- Смотрите на тренды (несколько прогонов)
- Сравнивайте с baseline (предыдущие успешные тесты)
- Учитывайте вариативность (±5% норма)
- Проверяйте все метрики, не только latency

❌ **DON'T:**
- Не принимайте решения по одному прогону
- Не игнорируйте Success Rate ради latency
- Не сравнивайте разные конфигурации YDB

## Troubleshooting

### Высокая latency только в начале теста

**Причина:** Недостаточный warmup  
**Решение:** Увеличить длительность warmup до 15-20 секунд

### Success Rate < 99.9% из-за timeout

**Причина:** Таймауты слишком короткие для текущей нагрузки  
**Решение:** Увеличить `READ_TIMEOUT` / `WRITE_TIMEOUT` в workflow

### Нестабильные результаты (разброс >10%)

**Причина:** Конкуренция за ресурсы в GitHub Actions runner  
**Решение:** Запустить несколько раз, усреднить результаты

### "Too many retries" ошибки

**Причина:** YDB перегружен или недостаточно нод  
**Решение:** Увеличить `ydb_database_node_count` в workflow

### Неравномерная нагрузка на ноды кластера

**Причина:** Количество потоков не соответствует размеру кластера  
**Диагностика:**
```bash
# Проверить утилизацию нод в YDB Monitoring
# Если разброс > 20% между нодами → проблема
```

**Решение:**
- Для 3 нод: уменьшить до 18-20 потоков
- Для 10 нод: увеличить до 50-60 потоков
- Проверить балансировку connection pool

### Высокая latency при увеличении размера кластера

**Причина:** Фиксированное количество потоков (30) недостаточно для утилизации большого кластера  
**Решение:** Масштабировать количество потоков пропорционально количеству нод

**Пример:**
```yaml
# Для 10 нод (требует доработки кода)
env:
  THREAD_COUNT: 60  
```

**Temporary workaround:** Запускать несколько инстансов теста параллельно

## Ссылки

- [YDB JDBC Driver](https://github.com/ydb-platform/ydb-jdbc-driver)
- [YDB Java SDK](https://github.com/ydb-platform/ydb-java-sdk)
- [YDB SLO Action](https://github.com/ydb-platform/ydb-slo-action)
- [YDB Documentation](https://ydb.tech/docs/)
- [Prometheus Client Java](https://github.com/prometheus/client_java)