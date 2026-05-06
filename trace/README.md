# Tracing Infrastructure

This folder contains local tracing infrastructure for examples:

- YDB
- OpenTelemetry Collector
- Tempo
- Prometheus
- Grafana
- QueryService demo (`trace/application/query-service`, programmatic OTel)
- TableClientImpl demo (`trace/application/table-client-impl`, programmatic OTel)
- JDBC demo (`trace/application/jdbc-trace`, programmatic OTel + `enableOpenTelemetryTracer` on the URL)
- Spring Data JPA / Hibernate 6 (`trace/application/spring-data-jpa-v6-trace`, Spring Boot 3 + same OTel wiring)
- Spring Data JDBC (`trace/application/spring-data-jdbc-trace`, Spring Boot 3 + Flyway + same OTel wiring)

Application runners are located in:

- `trace/application/query-service/`
- `trace/application/table-client-impl/`
- `trace/application/jdbc-trace/`
- `trace/application/spring-data-jpa-v6-trace/`
- `trace/application/spring-data-jdbc-trace/`

Prerequisite for SDK snapshots:

```bash
mvn install -DskipTests
```

Trace applications are pinned to `ydb-sdk-*` version `2.4.1-SNAPSHOT`. Demos that use JDBC (`jdbc-trace`, Spring Data
modules) expect a matching `ydb-jdbc-driver` snapshot (see each module `pom.xml`); install it locally if it is not in
your cache.

Spring Kotlin examples use Kotlin **2.2.x** (compatible with JDK 25 build hosts).

Build trace applications locally (uses your local Maven cache with snapshots):

```bash
mvn -B -f trace/application/query-service/pom.xml clean package -DskipTests
mvn -B -f trace/application/table-client-impl/pom.xml clean package -DskipTests
mvn -B -f trace/application/jdbc-trace/pom.xml clean package -DskipTests
mvn -B -f trace/application/spring-data-jpa-v6-trace/pom.xml clean package -DskipTests
mvn -B -f trace/application/spring-data-jdbc-trace/pom.xml clean package -DskipTests
```

## Run

```bash
docker compose -f trace/compose-e2e.yaml up -d
```

## Run QueryService demo

```bash
docker-compose -f trace/compose-e2e.yaml run --rm query-service-trace
```

## Run TableClientImpl demo

```bash
docker-compose -f trace/compose-e2e.yaml run --rm table-client-impl-trace
```

## Run JDBC demo

```bash
docker-compose -f trace/compose-e2e.yaml run --rm jdbc-trace
```

## Run Spring Data JPA (v6) demo

```bash
docker-compose -f trace/compose-e2e.yaml run --rm spring-data-jpa-v6-trace
```

## Run Spring Data JDBC demo

```bash
docker-compose -f trace/compose-e2e.yaml run --rm spring-data-jdbc-trace
```

## Run demos

```bash
docker-compose -f trace/compose-e2e.yaml run --rm query-service-trace
docker-compose -f trace/compose-e2e.yaml run --rm table-client-impl-trace
docker-compose -f trace/compose-e2e.yaml run --rm jdbc-trace
docker-compose -f trace/compose-e2e.yaml run --rm spring-data-jpa-v6-trace
docker-compose -f trace/compose-e2e.yaml run --rm spring-data-jdbc-trace
```

## Stop

```bash
docker compose -f trace/compose-e2e.yaml down
```
