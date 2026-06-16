# YDB Java SDK SLO workload

This module contains the workload application used by the [YDB SLO Action](https://github.com/ydb-platform/ydb-slo-action) to test the reliability of the YDB Java SDK under load and chaos.

It is a sibling of the SLO workloads in [`ydb-go-sdk`](https://github.com/ydb-platform/ydb-go-sdk/tree/master/tests/slo) and [`ydb-js-sdk`](https://github.com/ydb-platform/ydb-js-sdk/tree/main/tests/slo): the schema, queries and metrics are kept compatible so reports across SDKs are directly comparable.

## What it does

The workload runs three phases:

1. **Setup** — creates a partitioned KV table and prefills it with rows.
2. **Run** — drives concurrent read and write loops at fixed RPS for the configured duration. Each operation is timed and retried via `tech.ydb.query.tools.SessionRetryContext`; the outcome is recorded as Prometheus-compatible metrics that the action scrapes via OTLP.
3. **Teardown** — drops the workload table even if the run failed, so the cluster is left clean.

While the workload runs, the SLO action injects chaos (node restarts, network black holes, container pauses). The metrics show how well the SDK copes with those failures.

## Metrics

Every metric carries a `ref` label whose value is taken from the `WORKLOAD_REF` environment variable. This is how the report action separates the **current** PR run from the **baseline** run.

Names below are shown in Prometheus form (with underscores). Internally the workload uses the OpenTelemetry naming convention with dots (e.g. `sdk.operations.total`); the OTLP → Prometheus conversion replaces dots with underscores automatically, so this is what you see when you query Prometheus or write rules in `metrics.yaml`.

| Metric                              | Type            | Labels                                                  |
| ----------------------------------- | --------------- | ------------------------------------------------------- |
| `sdk_operations_total`              | counter         | `operation_type`, `operation_status`                    |
| `sdk_errors_total`                  | counter         | `operation_type`, `error_kind`                          |
| `sdk_retry_attempts_total`          | counter         | `operation_type`, `operation_status`                    |
| `sdk_pending_operations`            | up/down counter | `operation_type`                                        |
| `sdk_operation_latency_p50_seconds` | gauge           | `operation_type`, `operation_status` (always `success`) |
| `sdk_operation_latency_p95_seconds` | gauge           | `operation_type`, `operation_status` (always `success`) |
| `sdk_operation_latency_p99_seconds` | gauge           | `operation_type`, `operation_status` (always `success`) |

Latency percentiles are computed from per-operation HDR histograms and reflect only successful operations — failure latency is dominated by retry budgets and timeouts and would mask real SDK regressions during chaos. Counters (`sdk_operations_total`, `sdk_errors_total`) cover both branches, so availability is computed correctly.

## Inputs

The workload reads connection details and run parameters from environment variables provided by the action:

| Variable                        | Description                                      |
| ------------------------------- | ------------------------------------------------ |
| `YDB_CONNECTION_STRING`         | YDB connection string (preferred)                |
| `YDB_ENDPOINT` + `YDB_DATABASE` | Legacy, used if `YDB_CONNECTION_STRING` is unset |
| `WORKLOAD_REF`                  | Value of the `ref` label on every metric         |
| `WORKLOAD_NAME`                 | Workload name (used to compose the table name)   |
| `WORKLOAD_DURATION`             | Run duration in seconds                          |
| `OTEL_EXPORTER_OTLP_ENDPOINT`   | OTLP HTTP endpoint to push metrics to            |

KV-specific tunables are passed via the command line and parsed by JCommander:

```
--read-rps <int>            Target read RPS (default 1000)
--write-rps <int>           Target write RPS (default 100)
--read-timeout-ms <int>     Per-attempt read timeout in milliseconds (default 10000)
--write-timeout-ms <int>    Per-attempt write timeout in milliseconds (default 10000)
--prefill-count <long>      Rows to prefill before the run phase (default 1000)
--partition-size <int>      Auto-partitioning partition size in MB (default 1)
--min-partition-count <int> Minimum number of table partitions (default 6)
--max-partition-count <int> Maximum number of table partitions (default 1000)
--duration <int>            Override WORKLOAD_DURATION when > 0
```

Unknown flags are ignored, so the workload accepts `workload_current_command` strings designed for other SDKs without erroring.

## How CI uses this module

The CI lives in [`ydb-java-sdk/.github/workflows/slo.yml`](https://github.com/ydb-platform/ydb-java-sdk/blob/master/.github/workflows/slo.yml), not here. The flow is:

1. Check out the SDK PR (`current`) and the merge-base SDK commit (`baseline`).
2. Check out `ydb-java-examples` for the workload sources.
3. For each version, run `.github/scripts/build-slo-image.sh` from the SDK repo. The script assembles a build context with the SDK and examples checkouts side by side and feeds it to [`slo/Dockerfile`](Dockerfile), which:
   - Builds the SDK from source and installs it into an in-image local Maven repository.
   - Pins `ydb.sdk.version` in the examples parent pom to that version.
   - Builds the `slo` module against the freshly-installed SDK.
4. Pass the two images (`ydb-app-current`, `ydb-app-baseline`) to `ydb-platform/ydb-slo-action/init@v2`.
5. After the run, [`ydb-platform/ydb-slo-action/report@v2`](https://github.com/ydb-platform/ydb-slo-action) compares the two and posts a summary to the PR.

The build is fully self-contained — the SDK under test does not need to be published to a remote Maven repository.

## Building locally

The workload can be built standalone against a published SDK version. From the `ydb-java-examples` repository root:

```bash
mvn -pl slo -am -DskipTests package
```

The resulting jar is at `slo/target/ydb-slo-workload.jar`. To run it against a local YDB:

```bash
export YDB_CONNECTION_STRING="grpc://localhost:2136?database=/local"
export WORKLOAD_REF=local
export WORKLOAD_NAME=java-query-kv
export WORKLOAD_DURATION=60

java -jar slo/target/ydb-slo-workload.jar --read-rps 100 --write-rps 10 --prefill-count 100
```

If `OTEL_EXPORTER_OTLP_ENDPOINT` is not set, metrics are still recorded in-process but never exported — handy for verifying that the workload itself runs cleanly before pushing to CI.

## Files

```
slo/
├── Dockerfile                       Multi-stage build (SDK + workload)
├── pom.xml                          Maven module descriptor
├── README.md                        This file
└── src/main/
    ├── java/tech/ydb/slo/
    │   ├── Config.java              Reads action env vars
    │   ├── Main.java                Entry point
    │   ├── Metrics.java             OTLP metrics + HDR histograms
    │   └── kv/
    │       ├── KvWorkload.java      Setup/run/teardown loop
    │       ├── KvWorkloadParams.java JCommander-bound CLI flags
    │       ├── Row.java             Row data class
    │       └── RowGenerator.java    Random payload generator
    └── resources/
        └── log4j2.xml               Console logging config
```
