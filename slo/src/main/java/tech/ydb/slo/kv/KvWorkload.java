package tech.ydb.slo.kv;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.query.QueryClient;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.slo.Metrics;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

/**
 * Key-value workload for the SLO test.
 *
 * <p>The workload creates a partitioned table, prefills it with rows, and then
 * runs read and write loops at fixed RPS for the configured duration. Each
 * operation is timed and retried via {@link SessionRetryContext}; the outcome
 * is recorded into {@link Metrics} so the SLO action can compare current and
 * baseline runs.
 *
 * <p>Schema and queries mirror the KV workloads in the Go and JavaScript SDKs
 * so the produced metrics are directly comparable across SDKs.
 *
 * <p>Concurrency model: each operation type (read / write) gets a dedicated
 * thread pool sized to the configured RPS. Every worker thread pulls a permit
 * from a shared Guava {@link RateLimiter} and executes the operation inline.
 * There is no separate driver thread and no work queue, which removes the
 * unbounded backlog risk under chaos and keeps the worker count proportional
 * to the actual concurrency budget.
 */
public final class KvWorkload {
    private static final Logger logger = LoggerFactory.getLogger(KvWorkload.class);

    private static final String CREATE_TABLE_QUERY_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `%s` ("
            + "  hash Uint64,"
            + "  id Uint64,"
            + "  payload_str Utf8,"
            + "  payload_double Double,"
            + "  payload_timestamp Timestamp,"
            + "  payload_hash Uint64,"
            + "  PRIMARY KEY (hash, id)"
            + ") WITH ("
            + "  UNIFORM_PARTITIONS = %d,"
            + "  AUTO_PARTITIONING_BY_SIZE = ENABLED,"
            + "  AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,"
            + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,"
            + "  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d"
            + ")";

    private static final String DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE `%s`";

    private static final String WRITE_QUERY_TEMPLATE = ""
            + "DECLARE $id AS Uint64;"
            + "DECLARE $payload_str AS Utf8;"
            + "DECLARE $payload_double AS Double;"
            + "DECLARE $payload_timestamp AS Timestamp;"
            + "DECLARE $payload_hash AS Uint64;"
            + "UPSERT INTO `%s` ("
            + "  id, hash, payload_str, payload_double, payload_timestamp, payload_hash"
            + ") VALUES ("
            + "  $id,"
            + "  Digest::NumericHash($id),"
            + "  $payload_str,"
            + "  $payload_double,"
            + "  $payload_timestamp,"
            + "  $payload_hash"
            + ");";

    private static final String READ_QUERY_TEMPLATE = ""
            + "DECLARE $id AS Uint64;"
            + "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
            + "  FROM `%s`"
            + "  WHERE id = $id AND hash = Digest::NumericHash($id);";

    /*
     * Hard cap on the number of worker threads spawned for a single operation
     * type. The SLO targets a few hundred RPS in CI; allowing more workers
     * than this just wastes threads on JIT-warmup contention without
     * improving throughput.
     */
    private static final int MAX_WORKERS = 64;

    /*
     * Extra time, on top of the workload duration, given to worker pools to
     * complete their last in-flight operations before {@link #run()} forces
     * shutdown. Picked to be larger than the default per-attempt timeout so
     * a request that started just before the deadline can finish cleanly.
     */
    private static final long SHUTDOWN_GRACE_SECONDS = 30L;

    private final SessionRetryContext retryCtx;
    private final Metrics metrics;
    private final KvWorkloadParams params;
    private final String tablePath;

    private final RowGenerator generator;

    public KvWorkload(QueryClient queryClient, Metrics metrics, KvWorkloadParams params, String tablePath) {
        this.retryCtx = SessionRetryContext.create(queryClient).build();
        this.metrics = metrics;
        this.params = params;
        this.tablePath = tablePath;
        this.generator = new RowGenerator(params.prefillCount());
    }

    /*
     * Creates the table (if missing) and prefills it with
     * {@code params.prefillCount()} rows. Prefill uses a fixed-size thread pool
     * so we don't open thousands of sessions in parallel on slow runners.
     */
    public void setup() throws InterruptedException {
        logger.info("creating table {}", tablePath);
        Status createStatus = retryCtx.supplyResult(session ->
                session.createQuery(
                        String.format(
                                CREATE_TABLE_QUERY_TEMPLATE,
                                tablePath,
                                params.minPartitionCount(),
                                params.partitionSizeMb(),
                                params.minPartitionCount(),
                                params.maxPartitionCount()
                        ),
                        TxMode.NONE
                ).execute()
        ).join().getStatus();
        createStatus.expectSuccess("failed to create table " + tablePath);
        logger.info("table {} created", tablePath);

        logger.info("prefilling {} rows into {}", params.prefillCount(), tablePath);
        int parallelism = Math.min(MAX_WORKERS, Math.max(1, Math.min(params.prefillCount(), MAX_WORKERS)));
        ExecutorService prefillPool = Executors.newFixedThreadPool(
                parallelism, namedThreadFactory("slo-prefill-")
        );
        try {
            List<CompletableFuture<Status>> futures = new ArrayList<>();
            for (long i = 0; i < params.prefillCount(); i++) {
                final long id = i;
                futures.add(CompletableFuture.supplyAsync(
                        () -> writeRowSilently(RowGenerator.generate(id)),
                        prefillPool
                ));
            }

            int failed = 0;
            for (CompletableFuture<Status> f : futures) {
                Status s = f.join();
                if (!s.isSuccess()) {
                    failed++;
                    if (failed <= 5) {
                        logger.warn("prefill row failed: {}", s);
                    }
                }
            }
            if (failed > 0) {
                logger.warn("prefill completed with {} failed rows out of {}", failed, params.prefillCount());
            } else {
                logger.info("prefill completed");
            }
        } finally {
            prefillPool.shutdown();
            if (!prefillPool.awaitTermination(30, TimeUnit.SECONDS)) {
                prefillPool.shutdownNow();
            }
        }
    }

    /*
     * Runs the workload until the configured deadline or thread interruption.
     *
     * <p>Read and write workers run concurrently on dedicated thread pools.
     * Each worker pulls a permit from its rate limiter and executes the
     * operation inline, so there is no shared work queue and no driver
     * thread. Sub-zero RPS disables the corresponding loop entirely (useful
     * for write-only or read-only smoke tests).
     */
    public void run() throws InterruptedException {
        long durationSeconds = params.durationSeconds();
        long endNanos = durationSeconds > 0
                ? System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds)
                : Long.MAX_VALUE;

        // Track how many writes have completed so reads target a key-space
        // that's actually been populated. The generator itself was
        // constructed with nextId = prefillCount, so writes pick up where
        // prefill left off.
        AtomicLong writesIssued = new AtomicLong();

        int readWorkers = workerCount(params.readRps());
        int writeWorkers = workerCount(params.writeRps());

        if (readWorkers == 0 && writeWorkers == 0) {
            logger.warn("both read and write RPS are <= 0, run phase has nothing to do");
            return;
        }

        ExecutorService readPool = null;
        ExecutorService writePool = null;
        try {
            if (readWorkers > 0) {
                readPool = Executors.newFixedThreadPool(readWorkers, namedThreadFactory("slo-read-"));
                RateLimiter readLimiter = RateLimiter.create(params.readRps());
                for (int i = 0; i < readWorkers; i++) {
                    readPool.execute(() -> workerLoop(
                            endNanos, readLimiter,
                            () -> readOnce(writesIssued.get()),
                            "read"
                    ));
                }
            } else {
                logger.info("read RPS <= 0, skipping read workers");
            }

            if (writeWorkers > 0) {
                writePool = Executors.newFixedThreadPool(writeWorkers, namedThreadFactory("slo-write-"));
                RateLimiter writeLimiter = RateLimiter.create(params.writeRps());
                for (int i = 0; i < writeWorkers; i++) {
                    writePool.execute(() -> workerLoop(
                            endNanos, writeLimiter,
                            () -> {
                                writeOnce(generator.generate());
                                writesIssued.incrementAndGet();
                            },
                            "write"
                    ));
                }
            } else {
                logger.info("write RPS <= 0, skipping write workers");
            }

            // Wait for workers to drain naturally as they hit the deadline.
            // shutdown() lets in-flight ops finish; awaitTermination caps the
            // wait at duration + grace so the run phase can't hang past the
            // configured budget. Workers are stopped via shutdownNow() in
            // the finally block if they exceed the grace window.
            long graceNanos = TimeUnit.SECONDS.toNanos(SHUTDOWN_GRACE_SECONDS);
            long waitNanos = durationSeconds > 0
                    ? Math.max(0L, endNanos - System.nanoTime()) + graceNanos
                    : Long.MAX_VALUE;

            if (readPool != null) {
                readPool.shutdown();
            }
            if (writePool != null) {
                writePool.shutdown();
            }

            long readWaitNanos = waitNanos;
            if (readPool != null) {
                long started = System.nanoTime();
                if (!readPool.awaitTermination(readWaitNanos, TimeUnit.NANOSECONDS)) {
                    logger.warn("read pool did not drain within deadline, forcing shutdown");
                    readPool.shutdownNow();
                }
                waitNanos = Math.max(0L, waitNanos - (System.nanoTime() - started));
            }
            if (writePool != null) {
                if (!writePool.awaitTermination(waitNanos, TimeUnit.NANOSECONDS)) {
                    logger.warn("write pool did not drain within deadline, forcing shutdown");
                    writePool.shutdownNow();
                }
            }
        } finally {
            forceShutdown(readPool, "read pool");
            forceShutdown(writePool, "write pool");
        }
    }

    /*
     * Drops the workload table. Called from the {@code finally} block in
     * {@code Main} so the database is left clean even on failure.
     */
    public void teardown() {
        logger.info("dropping table {}", tablePath);
        Status status = retryCtx.supplyResult(session ->
                session.createQuery(
                        String.format(DROP_TABLE_QUERY_TEMPLATE, tablePath),
                        TxMode.NONE
                ).execute()
        ).join().getStatus();
        if (!status.isSuccess()) {
            logger.warn("failed to drop table {}: {}", tablePath, status);
        } else {
            logger.info("table {} dropped", tablePath);
        }
    }

    // --- internals ---------------------------------------------------------

    /*
     * Loops on a single worker thread until the deadline or interruption,
     * pacing each iteration through the shared rate limiter and running the
     * operation inline. No work queue is involved — backpressure comes
     * naturally from the limiter blocking the worker.
     */
    private void workerLoop(long endNanos, RateLimiter limiter, Runnable singleOp, String name) {
        while (System.nanoTime() < endNanos && !Thread.currentThread().isInterrupted()) {
            limiter.acquire();
            try {
                singleOp.run();
            } catch (Throwable t) {
                logger.warn("{} op threw unexpectedly: {}", name, t.toString());
            }
        }
    }

    /*
     * Computes the number of worker threads for a given RPS target.
     * Returns 0 for non-positive RPS so the caller skips the loop entirely.
     */
    private static int workerCount(int rps) {
        if (rps <= 0) {
            return 0;
        }
        return Math.min(MAX_WORKERS, Math.max(1, rps));
    }

    /*
     * Picks a random id in [0, keyspaceUpper) and reads it back from the table.
     * Reads target only ids known to exist (the prefilled range plus rows
     * written so far during this run), so a successful read always returns
     * a row and exercises the deserialization path.
     */
    private void readOnce(long writesObserved) {
        long upperBound = Math.max(1L, params.prefillCount() + writesObserved);
        long id = ThreadLocalRandom.current().nextLong(upperBound);

        Metrics.Span span = metrics.startOperation(Metrics.OperationType.READ);
        AtomicInteger attempts = new AtomicInteger();
        ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                .withRequestTimeout(Duration.ofMillis(params.readTimeoutMs()))
                .build();

        Result<QueryReader> result = retryCtx.supplyResult(session -> {
            attempts.incrementAndGet();
            return QueryReader.readFrom(session.createQuery(
                    String.format(READ_QUERY_TEMPLATE, tablePath),
                    TxMode.SNAPSHOT_RO,
                    Params.of("$id", PrimitiveValue.newUint64(id)),
                    settings
            ));
        }).join();

        int retryAttempts = Math.max(0, attempts.get() - 1);

        if (!result.getStatus().isSuccess()) {
            span.finishError(retryAttempts, classifyStatus(result.getStatus()));
            return;
        }

        // Touch the result set so we exercise the deserialization path.
        // For ids in the prefilled range the row is guaranteed to exist;
        // for ids in the just-written range it almost always exists, so
        // the absence branch is rare but harmless.
        QueryReader reader = result.getValue();
        if (reader.getResultSetCount() > 0) {
            ResultSetReader rs = reader.getResultSet(0);
            while (rs.next()) {
                rs.getColumn("id").getUint64();
            }
        }

        span.finishSuccess(retryAttempts);
    }

    private void writeOnce(Row row) {
        Metrics.Span span = metrics.startOperation(Metrics.OperationType.WRITE);
        AtomicInteger attempts = new AtomicInteger();

        Status status = writeRowInternal(row, attempts);
        int retryAttempts = Math.max(0, attempts.get() - 1);

        if (status.isSuccess()) {
            span.finishSuccess(retryAttempts);
        } else {
            span.finishError(retryAttempts, classifyStatus(status));
            logger.debug("write {} failed: {}", row.id(), status);
        }
    }

    /*
     * Writes a single row without recording metrics. Used during prefill so
     * the histogram of operation latencies is not polluted with bulk-load
     * timings.
     */
    private Status writeRowSilently(Row row) {
        return writeRowInternal(row, new AtomicInteger());
    }

    private Status writeRowInternal(Row row, AtomicInteger attempts) {
        ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                .withRequestTimeout(Duration.ofMillis(params.writeTimeoutMs()))
                .build();
        return retryCtx.supplyStatus(session -> {
            attempts.incrementAndGet();
            return session.createQuery(
                    String.format(WRITE_QUERY_TEMPLATE, tablePath),
                    TxMode.SERIALIZABLE_RW,
                    Params.of(
                            "$id", PrimitiveValue.newUint64(row.id()),
                            "$payload_str", PrimitiveValue.newText(row.payloadStr()),
                            "$payload_double", PrimitiveValue.newDouble(row.payloadDouble()),
                            "$payload_timestamp", PrimitiveValue.newTimestamp(row.payloadTimestamp()),
                            "$payload_hash", PrimitiveValue.newUint64(row.payloadHash())
                    ),
                    settings
            ).execute().thenApply(Result::getStatus);
        }).join();
    }

    private static String classifyStatus(Status status) {
        return "ydb/" + status.getCode().name().toLowerCase();
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }

    /*
     * Final cleanup for an executor service. The graceful shutdown is done
     * inline in {@link #run()} so deadlines line up with workload duration;
     * this method is the safety net invoked from the {@code finally} block,
     * forcing shutdown if the pool somehow survived.
     */
    private static void forceShutdown(ExecutorService pool, String name) {
        if (pool == null || pool.isTerminated()) {
            return;
        }
        logger.warn("{} still active in cleanup, forcing shutdown", name);
        pool.shutdownNow();
        try {
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("{} did not terminate after shutdownNow", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
