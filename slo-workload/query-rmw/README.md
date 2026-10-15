# Native Query transactional RMW

This is a separate native Query workload, not an adapter for JDBC, Spring Data
JDBC or JPA. It reuses the existing `Config`, `Metrics`, process namespace helper
and source-built SDK Dockerfile. The basic KV workloads are independent.

```sh
mvn -pl slo-workload/query-rmw -am package
java -jar slo-workload/query-rmw/target/ydb-slo-query-rmw-workload.jar \
    --duration 60 --writers 4
```

Connection, authentication, workload name/ref and OTLP configuration use the
same environment variables as the basic workloads. `WORKLOAD_DURATION` supplies
the default duration. `--duration 0` means unlimited. At least two writers are
required. The default is four.

Use `slo-workload/docker/Dockerfile.sdk` with
`WORKLOAD_MODULE=slo-workload/query-rmw`,
`WORKLOAD_JAR=ydb-slo-query-rmw-workload.jar`, and `COPY_LIBS=true` in the existing
two-repository source-build context. No shell wrapper or alternate SDK build
helper is required. The consuming workflow must explicitly select this module;
selecting the basic Query workload does not run RMW.

## Lifecycle and correctness

One invocation owns two tables named from the existing process-private UUID
namespace: `<namespace>_counters` and `<namespace>_operations`. It acknowledges
creation, seeds counters 0 and 1 at value/version 0, runs writers, waits for all
admitted operations, performs a fresh snapshot read, and acknowledges dropping
both tables. Cleanup is attempted after setup or run failure as well. A failed
drop fails an otherwise healthy invocation.

Each writer gets one admission in a bounded concurrent round. All writers in a
round target the same counter; consecutive rounds alternate counters. The next
round waits for all current operations, including retries, so a fast writer
cannot repeatedly overtake a contending peer. IDs are stable `writer:sequence`
strings. A writer's delta is `writer + 1`. Before each admission the runner retains
that ID, counter and delta in its own expected map.

`SessionRetryContext.supplyResult` encloses the entire interactive
`QueryTransaction` created with `SERIALIZABLE_RW`: read operation ID; if present,
verify its payload and commit without incrementing; otherwise read the counter,
update its value and version, insert the operation record, and commit. A retry
creates a new transaction but retains the same immutable operation. The SDK's
`idempotent(true)` declaration permits its existing ambiguous-result retries;
the transactional ledger makes that declaration valid. Retry budgets, backoff,
session acquisition timeout and per-query timeouts use the basic Query defaults.
No application retry loop or policy tuning is added.

The final check requires the exact admitted ID set and payloads, exactly two
counters, and each counter's exact total and version computed from the independent
expected map. It does not derive expected IDs or totals from the observed ledger.
This catches both sum-preserving corruption and a coherently missing ledger row
plus its counter update. Stream/status failures retain their native SDK cause.
The first final failure survives later validation and cleanup failures and exits
1. Recovered retries, normal duration and a requested runner stop exit 0 only
after in-flight settlement, validation and cleanup. Stop never cancels an
admitted SDK transaction.

## Attempt metrics

The legacy `sdk_retry_attempts_total` family counts technical attempts including
the first, not callback invocations or individual RPCs. `AttemptClient` is a
per-invocation, non-owning observation view of `QueryClient`. It increments on
every `SessionRetryContext` session acquisition, including acquisition failures
before the callback. The callback includes reads, writes and commit, so a failed
commit retried by the SDK starts another counted outer attempt. Scheduler,
tracer, pool and session behavior delegate unchanged. Counts cannot leak between
concurrent logical operations. One complete RMW is one logical write metric;
its component statements are not separate logical operations. Setup, final read
and cleanup also use the existing read/write metric families.

## Focused tests

```sh
JAVA_HOME=/path/to/jdk17 mvn -pl slo-workload/query-rmw -am package
```

Core tests exercise independent validation, duration, stop/settlement, fairness
and first-cause retention. Query tests retain the real SDK retry context, Result
composition and QueryReader, replacing only the Query transport boundary. They
exercise acquisition retry, commit retry, a committed-but-UNDETERMINED response,
deduplication and native failures. They are not a real database contention or
chaos receipt; that requires the consuming SDK workflow against a real YDB.
