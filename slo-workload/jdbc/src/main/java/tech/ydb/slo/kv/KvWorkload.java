package tech.ydb.slo.kv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
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

import tech.ydb.jdbc.exception.YdbStatusable;
import tech.ydb.slo.Metrics;

/**
 * Key-value workload for the SLO test, driving the YDB JDBC driver.
 *
 * <p>The workload creates a partitioned table, prefills it with rows, and then
 * runs read and write loops at fixed RPS for the configured duration. Each
 * operation is timed and retried with exponential backoff; the outcome is
 * recorded into {@link Metrics} so the SLO action can compare current and
 * baseline runs.
 *
 * <p>Schema and queries mirror the KV workloads in the Go and JavaScript SDKs
 * so the produced metrics are directly comparable across SDKs. Unlike the
 * query-service workload, the primary-key {@code hash} column is derived on
 * the client (see {@link #numericHash(long)}) instead of via the server-side
 * {@code Digest::NumericHash}, which keeps the parameterized JDBC statements
 * free of type-inference ambiguity.
 *
 * <p>Concurrency model: each operation type (read / write) gets a dedicated
 * thread pool sized to the configured RPS. Every worker thread owns its own
 * JDBC {@link Connection} (the YDB driver's connections are not thread-safe),
 * pulls a permit from a shared Guava {@link RateLimiter}, and executes the
 * operation inline. There is no separate driver thread and no work queue.
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
            + "UPSERT INTO `%s` ("
            + "  hash, id, payload_str, payload_double, payload_timestamp, payload_hash"
            + ") VALUES (?, ?, ?, ?, ?, ?)";

    private static final String READ_QUERY_TEMPLATE = ""
            + "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
            + "  FROM `%s`"
            + "  WHERE id = ? AND hash = ?";

    /*
     * Hard cap on the number of worker threads spawned for a single operation
     * type. The SLO targets a few hundred RPS in CI; allowing more workers
     * than this just wastes threads on JIT-warmup contention without
     * improving throughput.
     */
    private static final int MAX_WORKERS = 64;

    /*
     * Maximum number of attempts (initial + retries) per operation before it
     * is recorded as a failure. Mirrors the order of magnitude of the
     * query-service SessionRetryContext default.
     */
    private static final int MAX_ATTEMPTS = 10;

    private static final long INITIAL_BACKOFF_MS = 10L;
    private static final long MAX_BACKOFF_MS = 1_000L;

    /*
     * Extra time, on top of the workload duration, given to worker pools to
     * complete their last in-flight operations before {@link #run()} forces
     * shutdown.
     */
    private static final long SHUTDOWN_GRACE_SECONDS = 30L;

    private final String jdbcUrl;
    private final Properties connectionProperties;
    private final Metrics metrics;
    private final KvWorkloadParams params;
    private final String tablePath;
    private final RowGenerator generator;

    public KvWorkload(
            String jdbcUrl,
            Properties connectionProperties,
            Metrics metrics,
            KvWorkloadParams params,
            String tablePath
    ) {
        this.jdbcUrl = jdbcUrl;
        this.connectionProperties = connectionProperties;
        this.metrics = metrics;
        this.params = params;
        this.tablePath = tablePath;
        this.generator = new RowGenerator(params.prefillCount());
    }

    /*
     * Creates the table (if missing) and prefills it with
     * {@code params.prefillCount()} rows using a bounded pool of worker
     * connections.
     */
    public void setup() throws InterruptedException, SQLException {
        logger.info("creating table {}", tablePath);
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    CREATE_TABLE_QUERY_TEMPLATE,
                    tablePath,
                    params.minPartitionCount(),
                    params.partitionSizeMb(),
                    params.minPartitionCount(),
                    params.maxPartitionCount()
            ));
        }
        logger.info("table {} created", tablePath);

        if (params.prefillCount() <= 0) {
            logger.info("prefill count <= 0, skipping prefill");
            return;
        }

        logger.info("prefilling {} rows into {}", params.prefillCount(), tablePath);
        int parallelism = Math.min(MAX_WORKERS, Math.max(1, params.prefillCount()));
        ExecutorService prefillPool = Executors.newFixedThreadPool(
                parallelism, namedThreadFactory("slo-prefill-")
        );
        AtomicLong nextId = new AtomicLong(0);
        AtomicInteger failed = new AtomicInteger();
        try {
            for (int w = 0; w < parallelism; w++) {
                prefillPool.execute(() -> {
                    try (WorkerConnection wc = new WorkerConnection()) {
                        long id;
                        while ((id = nextId.getAndIncrement()) < params.prefillCount()) {
                            SQLException err = writeWithRetry(wc, RowGenerator.generate(id),
                                    params.writeTimeoutMs(), null);
                            if (err != null) {
                                int f = failed.incrementAndGet();
                                if (f <= 5) {
                                    logger.warn("prefill row {} failed: {}", id, err.toString());
                                }
                            }
                        }
                    }
                });
            }
        } finally {
            prefillPool.shutdown();
            if (!prefillPool.awaitTermination(5, TimeUnit.MINUTES)) {
                prefillPool.shutdownNow();
            }
        }
        if (failed.get() > 0) {
            logger.warn("prefill completed with {} failed rows out of {}",
                    failed.get(), params.prefillCount());
        } else {
            logger.info("prefill completed");
        }
    }

    /*
     * Runs the workload until the configured deadline or thread interruption.
     */
    public void run() throws InterruptedException {
        long durationSeconds = params.durationSeconds();
        long endNanos = durationSeconds > 0
                ? System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds)
                : Long.MAX_VALUE;

        // Track how many writes have completed so reads target a key-space
        // that's actually been populated. The generator was constructed with
        // nextId = prefillCount, so writes pick up where prefill left off.
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
                    readPool.execute(() -> readWorkerLoop(endNanos, readLimiter, writesIssued));
                }
            } else {
                logger.info("read RPS <= 0, skipping read workers");
            }

            if (writeWorkers > 0) {
                writePool = Executors.newFixedThreadPool(writeWorkers, namedThreadFactory("slo-write-"));
                RateLimiter writeLimiter = RateLimiter.create(params.writeRps());
                for (int i = 0; i < writeWorkers; i++) {
                    writePool.execute(() -> writeWorkerLoop(endNanos, writeLimiter, writesIssued));
                }
            } else {
                logger.info("write RPS <= 0, skipping write workers");
            }

            // Wait for workers to drain naturally as they hit the deadline.
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

            if (readPool != null) {
                long started = System.nanoTime();
                if (!readPool.awaitTermination(waitNanos, TimeUnit.NANOSECONDS)) {
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
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(DROP_TABLE_QUERY_TEMPLATE, tablePath));
            logger.info("table {} dropped", tablePath);
        } catch (SQLException e) {
            logger.warn("failed to drop table {}: {}", tablePath, e.toString());
        }
    }

    // --- worker loops ------------------------------------------------------

    private void readWorkerLoop(long endNanos, RateLimiter limiter, AtomicLong writesIssued) {
        try (WorkerConnection wc = new WorkerConnection()) {
            while (System.nanoTime() < endNanos && !Thread.currentThread().isInterrupted()) {
                limiter.acquire();
                try {
                    readOnce(wc, writesIssued.get());
                } catch (Throwable t) {
                    logger.warn("read op threw unexpectedly: {}", t.toString());
                }
            }
        }
    }

    private void writeWorkerLoop(long endNanos, RateLimiter limiter, AtomicLong writesIssued) {
        try (WorkerConnection wc = new WorkerConnection()) {
            while (System.nanoTime() < endNanos && !Thread.currentThread().isInterrupted()) {
                limiter.acquire();
                try {
                    writeOnce(wc, generator.generate());
                    writesIssued.incrementAndGet();
                } catch (Throwable t) {
                    logger.warn("write op threw unexpectedly: {}", t.toString());
                }
            }
        }
    }

    // --- single operations -------------------------------------------------

    private void readOnce(WorkerConnection wc, long writesObserved) {
        long upperBound = Math.max(1L, params.prefillCount() + writesObserved);
        long id = ThreadLocalRandom.current().nextLong(upperBound);
        long hash = numericHash(id);

        Metrics.Span span = metrics.startOperation(Metrics.OperationType.READ);
        int attempts = 0;
        SQLException last = null;
        while (attempts < MAX_ATTEMPTS) {
            attempts++;
            try {
                wc.read(id, hash, timeoutSeconds(params.readTimeoutMs()));
                span.finishSuccess(attempts - 1);
                return;
            } catch (SQLException e) {
                last = e;
                if (!isRetryable(e) || attempts >= MAX_ATTEMPTS) {
                    break;
                }
                wc.invalidateOnConnectionError(e);
                backoff(attempts);
            }
        }
        span.finishError(attempts - 1, classifyError(last));
        logger.debug("read {} failed: {}", id, last == null ? "?" : last.toString());
    }

    private void writeOnce(WorkerConnection wc, Row row) {
        Metrics.Span span = metrics.startOperation(Metrics.OperationType.WRITE);
        int[] attemptsOut = new int[1];
        SQLException err = writeWithRetry(wc, row, params.writeTimeoutMs(), attemptsOut);
        if (err == null) {
            span.finishSuccess(attemptsOut[0] - 1);
        } else {
            span.finishError(Math.max(0, attemptsOut[0] - 1), classifyError(err));
            logger.debug("write {} failed: {}", row.id(), err.toString());
        }
    }

    /*
     * Writes a single row with retry. When {@code attemptsOut} is non-null, the
     * total number of attempts is written to its first element. Returns
     * {@code null} on success or the last {@link SQLException} on failure.
     * Used both by the run phase (with metrics handled by the caller) and
     * prefill (silent).
     */
    private SQLException writeWithRetry(WorkerConnection wc, Row row, int timeoutMs, int[] attemptsOut) {
        long hash = numericHash(row.id());
        int attempts = 0;
        SQLException last = null;
        while (attempts < MAX_ATTEMPTS) {
            attempts++;
            try {
                wc.write(row, hash, timeoutSeconds(timeoutMs));
                if (attemptsOut != null) {
                    attemptsOut[0] = attempts;
                }
                return null;
            } catch (SQLException e) {
                last = e;
                if (!isRetryable(e) || attempts >= MAX_ATTEMPTS) {
                    break;
                }
                wc.invalidateOnConnectionError(e);
                backoff(attempts);
            }
        }
        if (attemptsOut != null) {
            attemptsOut[0] = attempts;
        }
        return last;
    }

    // --- helpers -----------------------------------------------------------

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, connectionProperties);
    }

    private static int timeoutSeconds(int timeoutMs) {
        return Math.max(1, (timeoutMs + 999) / 1000);
    }

    private static boolean isRetryable(SQLException e) {
        return e instanceof SQLRecoverableException || e instanceof SQLTransientException;
    }

    private static boolean isConnectionError(SQLException e) {
        return e instanceof SQLRecoverableException || e instanceof SQLTransientConnectionException;
    }

    private static String classifyError(SQLException e) {
        if (e == null) {
            return "unknown";
        }
        if (e instanceof YdbStatusable) {
            try {
                return "ydb/" + ((YdbStatusable) e).getStatus().getCode().name().toLowerCase();
            } catch (RuntimeException ignored) {
                // fall through to the generic classification
            }
        }
        return e.getClass().getSimpleName().toLowerCase();
    }

    private static void backoff(int attempt) {
        long delay = Math.min(MAX_BACKOFF_MS, INITIAL_BACKOFF_MS * (1L << Math.min(attempt - 1, 20)));
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Derives the primary-key {@code hash} column from {@code id} using a
     * SplitMix64-style mix. Reads and writes both call this so they always
     * target the same key. The exact function does not need to match YQL's
     * {@code Digest::NumericHash}; it only needs to be deterministic and
     * well distributed across partitions.
     * @param id row id
     * @return derived hash value
     */
    private static long numericHash(long id) {
        long z = id + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private static int workerCount(int rps) {
        if (rps <= 0) {
            return 0;
        }
        return Math.min(MAX_WORKERS, Math.max(1, rps));
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }

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

    /**
     * A JDBC connection owned by a single worker thread, together with lazily
     * prepared read/write statements. On a connection-level error the holder
     * is invalidated so the next operation transparently reconnects.
     */
    private final class WorkerConnection implements AutoCloseable {
        private Connection connection;
        private PreparedStatement readStmt;
        private PreparedStatement writeStmt;

        private Connection connection() throws SQLException {
            if (connection == null || connection.isClosed()) {
                connection = openConnection();
                readStmt = null;
                writeStmt = null;
            }
            return connection;
        }

        private PreparedStatement readStmt() throws SQLException {
            Connection conn = connection();
            if (readStmt == null) {
                readStmt = conn.prepareStatement(String.format(READ_QUERY_TEMPLATE, tablePath));
            }
            return readStmt;
        }

        private PreparedStatement writeStmt() throws SQLException {
            Connection conn = connection();
            if (writeStmt == null) {
                writeStmt = conn.prepareStatement(String.format(WRITE_QUERY_TEMPLATE, tablePath));
            }
            return writeStmt;
        }

        void read(long id, long hash, int timeoutSeconds) throws SQLException {
            PreparedStatement stmt = readStmt();
            stmt.setQueryTimeout(timeoutSeconds);
            stmt.setLong(1, id);
            stmt.setLong(2, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                // Touch the result set so we exercise the deserialization path.
                while (rs.next()) {
                    rs.getLong("id");
                }
            }
        }

        void write(Row row, long hash, int timeoutSeconds) throws SQLException {
            PreparedStatement stmt = writeStmt();
            stmt.setQueryTimeout(timeoutSeconds);
            stmt.setLong(1, hash);
            stmt.setLong(2, row.id());
            stmt.setString(3, row.payloadStr());
            stmt.setDouble(4, row.payloadDouble());
            stmt.setTimestamp(5, Timestamp.from(row.payloadTimestamp()));
            stmt.setLong(6, row.payloadHash());
            stmt.executeUpdate();
        }

        void invalidateOnConnectionError(SQLException e) {
            if (isConnectionError(e)) {
                close();
            }
        }

        @Override
        public void close() {
            closeQuietly(readStmt);
            closeQuietly(writeStmt);
            closeQuietly(connection);
            readStmt = null;
            writeStmt = null;
            connection = null;
        }

        private void closeQuietly(AutoCloseable closeable) {
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
    }
}
