package tech.ydb.slo.core.kv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.slo.core.Metrics;

public final class WorkloadRunner {
    private static final Logger logger = LoggerFactory.getLogger(WorkloadRunner.class);

    private final KvClient client;
    private final Metrics metrics;
    private final KvWorkloadParams params;
    private final String tablePath;
    private final RowGenerator generator;
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

    public WorkloadRunner(KvClient client, Metrics metrics, KvWorkloadParams params, String tablePath) {
        this.client = client;
        this.metrics = metrics;
        this.params = params;
        this.tablePath = tablePath;
        this.generator = new RowGenerator(params.prefillCount());
    }

    public void setup() throws Exception {
        logger.info("creating table {}", tablePath);
        client.createTable(params, tablePath);
        logger.info("table {} created", tablePath);

        if (params.prefillCount() <= 0) {
            logger.info("prefill count <= 0, skipping prefill");
            return;
        }

        logger.info("prefilling {} rows into {}", params.prefillCount(), tablePath);
        int parallelism = Math.min(params.maxWorkers(), Math.max(1, params.prefillCount()));
        ExecutorService prefillPool = Executors.newFixedThreadPool(
                parallelism, namedThreadFactory("slo-prefill-")
        );
        AtomicLong nextId = new AtomicLong(0);
        try {
            for (int w = 0; w < parallelism; w++) {
                prefillPool.execute(() -> {
                    try (KvSession session = client.openSession()) {
                        long id;
                        while (firstFailure.get() == null &&
                                (id = nextId.getAndIncrement()) < params.prefillCount()) {
                            OpOutcome outcome = session.write(
                                    RowGenerator.generate(id), params.writeTimeoutMs());
                            if (!outcome.isSuccess()) {
                                recordFailure(outcome);
                                return;
                            }
                        }
                    } catch (Throwable e) {
                        firstFailure.compareAndSet(null, e);
                        logger.error("prefill worker failed to open session: {}", e.toString());
                    }
                });
            }
        } finally {
            prefillPool.shutdown();
            if (!prefillPool.awaitTermination(5, TimeUnit.MINUTES)) {
                firstFailure.compareAndSet(null, new IllegalStateException("prefill workers did not finish"));
                prefillPool.shutdownNow();
            }
        }

        throwIfFailed();
        logger.info("prefill completed");
    }

    public void run() throws InterruptedException {
        long durationSeconds = params.durationSeconds();
        long endNanos = durationSeconds > 0
                ? System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds)
                : Long.MAX_VALUE;

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

            long graceNanos = TimeUnit.SECONDS.toNanos(params.shutdownTimeSeconds());
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
                    firstFailure.compareAndSet(null, new IllegalStateException("read pool did not drain"));
                    logger.warn("read pool did not drain within deadline, forcing shutdown");
                    readPool.shutdownNow();
                }
                waitNanos = Math.max(0L, waitNanos - (System.nanoTime() - started));
            }
            if (writePool != null) {
                if (!writePool.awaitTermination(waitNanos, TimeUnit.NANOSECONDS)) {
                    firstFailure.compareAndSet(null, new IllegalStateException("write pool did not drain"));
                    logger.warn("write pool did not drain within deadline, forcing shutdown");
                    writePool.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            throwIfFailed();
            throw e;
        } finally {
            forceShutdown(readPool, "read pool");
            forceShutdown(writePool, "write pool");
        }
        throwIfFailed();
    }

    public void teardown() {
        logger.info("dropping table {}", tablePath);
        client.dropTable(tablePath);
    }

    private void readWorkerLoop(long endNanos, RateLimiter limiter, AtomicLong writesIssued) {
        try (KvSession session = client.openSession()) {
            while (firstFailure.get() == null && !Thread.currentThread().isInterrupted()) {
                long remaining = endNanos - System.nanoTime();
                if (remaining <= 0) {
                    return;
                }

                if (!limiter.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
                    return;
                }
                if (firstFailure.get() != null || System.nanoTime() >= endNanos) {
                    return;
                }
                try {
                    readOnce(session, writesIssued.get());
                } catch (Throwable t) {
                    firstFailure.compareAndSet(null, t);
                    logger.warn("read op threw unexpectedly: {}", t.toString());
                    return;
                }
            }
        } catch (Throwable e) {
            firstFailure.compareAndSet(null, e);
            logger.warn("read worker failed to open session: {}", e.toString());
        }
    }

    private void writeWorkerLoop(long endNanos, RateLimiter limiter, AtomicLong writesIssued) {
        try (KvSession session = client.openSession()) {
            while (firstFailure.get() == null && !Thread.currentThread().isInterrupted()) {
                long remaining = endNanos - System.nanoTime();
                if (remaining <= 0) {
                    return;
                }
                if (!limiter.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
                    return;
                }
                if (firstFailure.get() != null || System.nanoTime() >= endNanos) {
                    return;
                }
                try {
                    writeOnce(session, generator.generate());
                    writesIssued.incrementAndGet();
                } catch (Throwable t) {
                    firstFailure.compareAndSet(null, t);
                    logger.warn("write op threw unexpectedly: {}", t.toString());
                    return;
                }
            }
        } catch (Throwable e) {
            firstFailure.compareAndSet(null, e);
            logger.warn("write worker failed to open session: {}", e.toString());
        }
    }

    private void readOnce(KvSession session, long writesObserved) {
        long upperBound = Math.max(1L, params.prefillCount() + writesObserved);
        long id = ThreadLocalRandom.current().nextLong(upperBound);

        Metrics.Span span = metrics.startOperation(Metrics.OperationType.READ);
        OpOutcome outcome = session.read(id, params.readTimeoutMs());
        if (outcome.isSuccess()) {
            span.finishSuccess(outcome.retryAttempts());
        } else {
            recordFailure(outcome);
            span.finishError(outcome.retryAttempts(), outcome.errorKind());
            logger.debug("read {} failed: {}", id, outcome.errorKind());
        }
    }

    private void writeOnce(KvSession session, Row row) {
        Metrics.Span span = metrics.startOperation(Metrics.OperationType.WRITE);
        OpOutcome outcome = session.write(row, params.writeTimeoutMs());
        if (outcome.isSuccess()) {
            span.finishSuccess(outcome.retryAttempts());
        } else {
            recordFailure(outcome);
            span.finishError(outcome.retryAttempts(), outcome.errorKind());
            logger.debug("write {} failed: {}", row.id(), outcome.errorKind());
        }
    }

    private void recordFailure(OpOutcome outcome) {
        firstFailure.compareAndSet(null, outcome.cause() != null ? outcome.cause()
                : new IllegalStateException("Operation failed: " + outcome.errorKind()));
    }

    private void throwIfFailed() {
        Throwable failure = firstFailure.get();
        if (failure != null) {
            throw new IllegalStateException("Workload failed", failure);
        }
    }

    private int workerCount(int rps) {
        if (rps <= 0) {
            return 0;
        }
        return Math.min(params.maxWorkers(), Math.max(1, rps));
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
}
