# JDBC SLO workload

A plain-JDBC SLO workload that exercises the
[YDB JDBC driver](https://github.com/ydb-platform/ydb-jdbc-driver) under load
and chaos. It mirrors the structure and metrics contract of the SDK SLO
workload in [`../../slo`](../../slo), so reports are directly comparable.

> See the [parent README](../README.md) for the shared metrics, environment
> variables, CLI flags and CI flow.

## What it does

The workload runs as a standalone jar (`tech.ydb.slo.Main`) and goes through
three phases against a partitioned KV table:

1. **Setup** — `CREATE TABLE IF NOT EXISTS` plus a prefill of `--prefill-count`
   rows.
2. **Run** — dedicated read and write thread pools, each paced by a Guava
   `RateLimiter` to the target RPS, running until the configured duration.
3. **Teardown** — `DROP TABLE`.

Every worker thread owns its own JDBC `Connection` (the driver's connections
are not thread-safe) and reuses prepared statements. On a connection-level
error the connection is transparently reopened on the next attempt.

## Schema

```
hash              Uint64  -- primary key, derived from id on the client
id                Uint64  -- primary key
payload_str       Utf8
payload_double    Double
payload_timestamp Timestamp
payload_hash      Uint64
```

The primary-key `hash` column is derived from `id` with a SplitMix64-style mix
(`KvWorkload#numericHash`) so reads and writes target the same key without
relying on server-side YQL builtins inside parameterized statements.

## Retries

Operations are retried with exponential backoff (up to 10 attempts). An error
is considered retryable when the driver throws a `SQLRecoverableException` or
`SQLTransientException` (which covers the driver's
`YdbRetryableException`, `YdbConditionallyRetryableException`,
`YdbUnavailbaleException` and `YdbTimeoutException`). The number of retries is
recorded in `sdk_retry_attempts_total`, and the failure reason is reported via
the `error_kind` label on `sdk_errors_total` (using the YDB status code when
available).

## Files

```
jdbc/
├── Dockerfile
├── pom.xml
├── README.md
└── src/main/
    ├── java/tech/ydb/slo/
    │   ├── Config.java              Reads env vars, resolves the JDBC URL
    │   ├── Main.java                Entry point
    │   ├── Metrics.java             OTLP metrics + HDR histograms
    │   └── kv/
    │       ├── KvWorkload.java      Setup/run/teardown loop over JDBC
    │       ├── KvWorkloadParams.java JCommander-bound CLI flags
    │       ├── Row.java             Row data class
    │       └── RowGenerator.java    Random payload generator
    └── resources/
        └── log4j2.xml               Console logging config
```

## Building and running locally

```bash
# From the repository root
mvn -pl slo-workload/jdbc -am -DskipTests package

export YDB_CONNECTION_STRING="grpc://localhost:2136/local"
export WORKLOAD_REF=local
export WORKLOAD_NAME=java-slo-jdbc

java -jar slo-workload/jdbc/target/ydb-slo-jdbc-workload.jar \
  --duration 60 --read-rps 100 --write-rps 10 --prefill-count 100
```

Build the container image (context is the repository root):

```bash
docker build -f slo-workload/jdbc/Dockerfile -t ydb-slo-jdbc-workload .
```
