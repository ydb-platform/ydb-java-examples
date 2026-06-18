# AGENTS.md — slo-workload

Guide for AI agents working on the YDB SLO workloads. Pairs with
[ydb-platform/ydb-slo-action](https://github.com/ydb-platform/ydb-slo-action),
which is the GitHub Action that actually runs these images.

## What this module is

This is the **workload-side half** of the ydb-slo-action contract. The action
deploys a YDB cluster + Prometheus + a chaos monkey, then runs two copies of
your workload container (current vs baseline) inside the same Docker network
and compares their metrics. Everything here exists to satisfy that contract.

Concretely:

- `core/` — shared harness (`Config`, `Launcher`, `Metrics`, `kv/*`). Reads
  the env vars the action injects, drives setup → run → teardown, pushes OTLP
  metrics with `ref={current|baseline}` labels.
- `query/`, `jdbc/`, `spring-data-jdbc/`, `spring-data-jpa/` — one workload per
  client under test. Each module is ~thin: a `Main` that wires a `KvClient`
  implementation into the shared `Launcher`.
- `docker/Dockerfile.sdk` — single shared Dockerfile used by the **SDK**
  CI (`ydb-java-sdk`). Picks the workload module via build args.
- `<module>/Dockerfile` — per-workload Dockerfile used by the **JDBC driver**
  CI (`ydb-jdbc-driver`). Optionally builds the driver from source when its
  checkout is present in the build context.

## Two-repo build pattern (this is the load-bearing trick)

The CI workflows in `ydb-java-sdk` and `ydb-jdbc-driver` need to test SDK/driver
code **as it is in the PR** — not whatever version is pinned in this repo's
`pom.xml`. The pattern is:

1. The component-under-test repo has the workflow file (`slo.yml` / `slo.yaml`)
   and a `build-slo-image.sh` script.
2. The workflow checks out **three** trees with `actions/checkout`:
   - current SDK/driver (the PR's HEAD)
   - baseline SDK/driver (the merge-base commit with `master`)
   - `ydb-java-examples@master` (this repo)
3. `build-slo-image.sh` assembles a temporary Docker build context by
   hard-linking both trees into one directory, then runs `docker build` with
   the Dockerfile from this repo.
4. The Dockerfile sees both trees, installs the component-under-test from
   source (`mvn install`), reads back its version via `help:evaluate`, pins
   the workload's dependency property to that exact version with
   `versions:set-property`, then packages the workload module.

Net effect: a workload jar built against the **exact** SDK/driver under test,
not a stale release pinned in this repo. The baseline image is built the same
way against the merge-base, so any chaos regression you see in the report is
isolated to the PR's changes — the rest of the stack is identical.

Two flavors of this Dockerfile pattern live in this repo:

- **`docker/Dockerfile.sdk`** — copies `ydb-java-sdk/` and `ydb-java-examples/`
  side-by-side, installs the SDK first, then sets `ydb.sdk.version` on the
  workload and packages the module named by `WORKLOAD_MODULE`. One image for
  all four SDK workloads.
- **`<module>/Dockerfile`** — copies the examples repo at context root and
  `ydb-jdbc-driver/` next to it. If the driver checkout is present, install it
  and pin `ydb.jdbc.version`; otherwise fall back to `YDB_JDBC_VERSION` or the
  property already in the POM. One Dockerfile per workload (the JDBC line has
  three workloads — `jdbc`, `spring-data-jdbc`, `spring-data-jpa`).

The shapes differ because of which `pom.xml` property is being pinned, and
because the SDK workflow shares a single Dockerfile across the matrix while
the JDBC workflow uses per-module Dockerfiles. Both shapes use the same
build-context trick.

## The ydb-slo-action contract

The workload reads environment variables (set by the action) and writes
metrics. The contract is asymmetric: the action provides connection details,
the workload provides metrics. No filesystem state crosses the boundary.

### Inputs the action provides

| Env var | Used for |
| --- | --- |
| `YDB_JDBC_URL` / `YDB_CONNECTION_STRING` / `YDB_ENDPOINT`+`YDB_DATABASE` | Connection (first set wins) |
| `YDB_TOKEN` | Optional auth token |
| `WORKLOAD_REF` | Value of the `ref` label on every metric (action sets this to `current` or `baseline`) |
| `WORKLOAD_NAME` | Workload name (also the table-name prefix) |
| `WORKLOAD_DURATION` | Run duration in seconds (0 = unlimited) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Prometheus OTLP HTTP receiver |

Plus per-workload CLI flags via JCommander (see README).

### Metrics the workload produces

All metrics carry `ref=<WORKLOAD_REF>`. The action's report compares the two
`ref` values pairwise.

- `sdk_operations_total{operation_type, operation_status}` — counter
- `sdk_errors_total{operation_type, error_kind}` — counter
- `sdk_retry_attempts_total{operation_type, operation_status}` — counter
- `sdk_pending_operations{operation_type}` — up/down counter
- `sdk_operation_latency_p{50,95,99}_seconds{operation_type, operation_status=success}` — gauges, fed from per-second HdrHistogram snapshots, reset after each scrape

Latency percentiles cover **only** successful operations on purpose: failure
latency is dominated by retry budgets/timeouts and would mask real
regressions during chaos. The total/error counters cover both branches, so
availability still computes correctly.

## When adding a new workload

1. Create a module next to `jdbc/`. Reuse `core/` (don't re-implement Config,
   Metrics, Launcher, the KV runner — the cross-implementation comparability
   in the report depends on every workload writing identical rows via
   `RowGenerator.numericHash`).
2. Implement `KvClient` + a thin `Main` that calls
   `Launcher.launch(programName, defaultWorkloadName, args, factory)`.
3. Add a `Dockerfile` next to the module that installs the component under
   test from source (mirror `jdbc/Dockerfile`), or extend `Dockerfile.sdk`
   and pass `WORKLOAD_MODULE=slo-workload/<name>` from CI.
4. Register the module in `pom.xml` under the `jdk17-slo-workload` profile.
5. Wire it into the SLO workflow of the component-under-test repository
   (`ydb-java-sdk/.github/workflows/slo.yml` or
   `ydb-jdbc-driver/.github/workflows/slo.yaml`). This repo does **not** run
   SLO CI by itself — there's no YDB cluster here.

## Local sanity checks

```bash
# Build a single workload module
mvn -pl slo-workload/jdbc -am -DskipTests package

# Smoke-run against a local YDB without OTLP export
export YDB_CONNECTION_STRING="grpc://localhost:2136/local"
export WORKLOAD_REF=local
export WORKLOAD_NAME=java-slo-jdbc
java -jar slo-workload/jdbc/target/ydb-slo-jdbc-workload.jar \
    --duration 60 --read-rps 100 --write-rps 10 --prefill-count 100
```

If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, metrics are recorded in-process
but not pushed — useful for verifying the workload runs cleanly before
pushing to CI.

## Things that bite

- **The `export` stage in `Dockerfile.sdk` must `FROM workload-build`**, not
  the bare Maven image. Starting from a clean image silently drops `/src/`
  and breaks the COPY in the runtime stage. (Cost a fix commit; don't repeat
  it.)
- **`WORKLOAD_REF=unknown`** is the silent default if the env var is missing.
  Locally that's fine; in CI it would merge current and baseline series. The
  action always sets it, but watch out when reproducing CI behaviour
  manually.
- **Prefill failure threshold is 50%**. If half the writes during prefill
  fail, the runner refuses to start the read loop — empty key-space means
  meaningless read latency. Don't lower the threshold to "fix" a flaky test.
- **`acceptUnknownOptions(false)`** on JCommander is intentional. A typo in
  the ydb-slo-action `workload_*_command` input should fail loudly, not
  silently fall back to defaults.
- **HdrHistograms are `AtomicHistogram`** for lock-free hot-path recording.
  Don't swap them back to `synchronized` Histogram — the contention shows up
  at high RPS.
