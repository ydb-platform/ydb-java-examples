# YDB SLO Workload Tests

This module hosts SLO (Service Level Objective) workloads that test the
reliability of YDB Java clients under load and chaos using the
[YDB SLO action](https://github.com/ydb-platform/ydb-slo-action).

Each submodule is a self-contained, runnable workload that follows the same
contract: it reads its configuration from environment variables, runs
setup/run/teardown phases, and pushes OpenTelemetry (OTLP) metrics that the
action scrapes and compares between the current PR run and a baseline run.

Shared harness code lives in [`core`](core) (`Config`, `Metrics`, KV row
model, rate-limited runner). Every workload plugs a `KvClient` adapter into
that runner so all of them emit the same metric contract.

| Module | Component under test | Description |
| --- | --- | --- |
| [`query`](query) | `ydb-java-sdk` (query client) | Native SDK KV workload |
| [`jdbc`](jdbc) | `ydb-jdbc-driver` | Plain JDBC KV workload (no framework) |
| [`spring-data-jdbc`](spring-data-jdbc) | `ydb-jdbc-driver` + `spring-data-jdbc-ydb` + `spring-ydb-retry` | Spring Data JDBC KV workload |
| [`spring-data-jpa`](spring-data-jpa) | `ydb-jdbc-driver` + Hibernate 6 + `spring-ydb-retry` | Spring Data JPA KV workload |

## How a workload behaves

Every workload runs three phases:

1. **Setup** — creates a partitioned KV table and prefills it with rows.
2. **Run** — drives concurrent read and write loops at fixed RPS for the
   configured duration. Each operation is timed and retried; the outcome is
   recorded as OTLP metrics.
3. **Teardown** — drops the workload table even if the run failed, so the
   cluster is left clean.

While the workload runs, the SLO action injects chaos (node restarts, network
black holes, container pauses). The metrics show how well the client copes.

## Metrics

Every metric carries a `ref` label taken from the `WORKLOAD_REF` environment
variable, which lets the report action separate the **current** run from the
**baseline** run. Names are shown below in Prometheus form (dots become
underscores during the OTLP → Prometheus conversion).

| Metric | Type | Labels |
| --- | --- | --- |
| `sdk_operations_total` | counter | `operation_type`, `operation_status` |
| `sdk_errors_total` | counter | `operation_type`, `error_kind` |
| `sdk_retry_attempts_total` | counter | `operation_type`, `operation_status` |
| `sdk_pending_operations` | up/down counter | `operation_type` |
| `sdk_operation_latency_p50_seconds` | gauge | `operation_type`, `operation_status` (always `success`) |
| `sdk_operation_latency_p95_seconds` | gauge | `operation_type`, `operation_status` (always `success`) |
| `sdk_operation_latency_p99_seconds` | gauge | `operation_type`, `operation_status` (always `success`) |

Latency percentiles are computed from per-operation HDR histograms and reflect
only successful operations — failure latency is dominated by retry budgets and
timeouts and would mask real regressions during chaos. Counters cover both
branches, so availability is computed correctly.

## Inputs

Connection details and run parameters come from environment variables:

| Variable | Description |
| --- | --- |
| `YDB_JDBC_URL` | Full JDBC URL (`jdbc:ydb:...`), used verbatim if set |
| `YDB_CONNECTION_STRING` | YDB connection string; prefixed with `jdbc:ydb:` |
| `YDB_ENDPOINT` + `YDB_DATABASE` | Used to compose the connection string if the above are unset |
| `YDB_TOKEN` | Optional auth token |
| `WORKLOAD_REF` | Value of the `ref` label on every metric |
| `WORKLOAD_NAME` | Workload name (also part of the table name) |
| `WORKLOAD_DURATION` | Run duration in seconds |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP HTTP endpoint to push metrics to |

KV tunables are passed on the command line and parsed by JCommander:

```
--read-rps <int>            Target read RPS (default 1000)
--write-rps <int>           Target write RPS (default 100)
--read-timeout-ms <int>     Per-attempt read timeout in ms (default 10000)
--write-timeout-ms <int>    Per-attempt write timeout in ms (default 10000)
--prefill-count <int>       Rows to prefill before the run phase (default 1000)
--partition-size <int>      Auto-partitioning partition size in MB (default 1)
--min-partition-count <int> Minimum number of table partitions (default 6)
--max-partition-count <int> Maximum number of table partitions (default 1000)
--duration <int>            Override WORKLOAD_DURATION when > 0
```

Unknown flags are ignored, so a workload accepts command strings designed for
other SDKs without erroring.

## How CI uses this module

This repository only hosts the workload sources. The CI that actually runs them
lives in the repository of the component under test — for `jdbc` that is
[`ydb-jdbc-driver`](https://github.com/ydb-platform/ydb-jdbc-driver) — mirroring
how the SDK SLO workload in [`../slo`](../slo) is driven from the
`ydb-java-sdk` repository rather than from here.

The driver's workflow, via [`ydb-platform/ydb-slo-action`](https://github.com/ydb-platform/ydb-slo-action):

1. checks out the driver under test (current and baseline) and this repository
   for the workload sources;
2. `ydb-platform/ydb-slo-action/init` deploys a YDB cluster (storage + database
   nodes), Prometheus with an OTLP receiver, and a chaos monkey, exposing the
   database node IPs and the OTLP endpoint as step outputs;
3. builds the workload jar and runs it, pointing `YDB_CONNECTION_STRING` at a
   database node and `OTEL_EXPORTER_OTLP_ENDPOINT` at the Prometheus OTLP
   receiver;
4. `ydb-platform/ydb-slo-action/report` compares the current run against the
   baseline and posts a summary to the PR.

## Building locally

From the `ydb-java-examples` repository root:

```bash
mvn -pl slo-workload/jdbc -am -DskipTests package
```

The resulting jar is at
`slo-workload/jdbc/target/ydb-slo-jdbc-workload.jar`. To run it against a
local YDB:

```bash
export YDB_CONNECTION_STRING="grpc://localhost:2136/local"
export WORKLOAD_REF=local
export WORKLOAD_NAME=java-slo-jdbc

java -jar slo-workload/jdbc/target/ydb-slo-jdbc-workload.jar \
  --duration 60 --read-rps 100 --write-rps 10 --prefill-count 100
```

If `OTEL_EXPORTER_OTLP_ENDPOINT` is not set, metrics are still recorded
in-process but never exported — handy for verifying that the workload runs
cleanly before pushing to CI.

## Adding a new workload

1. Create a module next to `jdbc` (e.g. `spring-data-jpa`).
2. Reuse `Config`, `Metrics`, and the `kv` package; replace only the data
   access layer with the framework under test.
3. Register the module in `slo-workload/pom.xml` and wire it into the SLO
   workflow of the component under test (in its own repository).

## Links

- [SDK SLO workload (reference)](../slo)
- [YDB SLO Action](https://github.com/ydb-platform/ydb-slo-action)
- [YDB JDBC Driver](https://github.com/ydb-platform/ydb-jdbc-driver)
- [YDB Documentation](https://ydb.tech/docs/)
