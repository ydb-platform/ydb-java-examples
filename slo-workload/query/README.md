# Query SLO workload

A YDB Java SDK SLO workload that exercises the native
[query client](https://github.com/ydb-platform/ydb-java-sdk) under load and
chaos. Schema and metric contract match the JDBC and Spring-Data workloads in
this directory so reports across SDK flavors are directly comparable.

> See the [parent README](../README.md) for the shared metrics, environment
> variables, CLI flags and CI flow.

## What it does

Standalone jar (`tech.ydb.slo.Main`) that runs three phases against a
partitioned KV table:

1. **Setup** — `CREATE TABLE IF NOT EXISTS` plus a prefill of `--prefill-count`
   rows. Setup aborts loudly if more than half the prefill writes fail, so a
   misconfigured cluster never produces green metrics on an empty table.
2. **Run** — dedicated read and write thread pools, each paced by a Guava
   `RateLimiter` to the target RPS, running until the configured duration.
3. **Teardown** — `DROP TABLE IF EXISTS`.

Operations go through `tech.ydb.query.tools.SessionRetryContext`. Its retry
budget is configured from `--max-attempts` so the query workload exhibits the
same retry pressure as the JDBC workload under chaos.

## Schema

```
hash              Uint64  -- primary key, computed client-side from id
id                Uint64  -- primary key
payload_str       Utf8
payload_double    Double
payload_timestamp Timestamp
payload_hash      Uint64
```

The primary-key `hash` column is derived from `id` with the same SplitMix64
mix as every other workload in this module (`RowGenerator.numericHash`), so a
table written by `query` is byte-compatible with one written by `jdbc` /
`spring-data-jdbc` / `spring-data-jpa`.

## Files

```
query/
├── Dockerfile
├── pom.xml
├── README.md
└── src/main/
    ├── java/tech/ydb/slo/
    │   ├── Main.java                 Entry point
    │   └── query/
    │       └── QueryKvClient.java    KvClient: SessionRetryContext-backed ops
    └── resources/
        └── log4j2.xml                Console logging config
```

The shared harness (`Config`, `Metrics`, `KvWorkloadParams`, `Row`,
`RowGenerator`, `WorkloadRunner`, `Launcher`) lives in
[`../core`](../core/src/main/java/tech/ydb/slo/core).

## Building and running locally

```bash
# From the repository root
mvn -pl slo-workload/query -am -DskipTests package

export YDB_CONNECTION_STRING="grpc://localhost:2136/local"
export WORKLOAD_REF=local
export WORKLOAD_NAME=java-slo-query

java -jar slo-workload/query/target/ydb-slo-query-workload.jar \
  --duration 60 --read-rps 100 --write-rps 10 --prefill-count 100
```

Build the container image (context is the repository root):

```bash
docker build -f slo-workload/query/Dockerfile -t ydb-slo-query-workload .
```
