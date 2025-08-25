package tech.ydb.apps;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.exception.YdbStatusable;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class AppMetrics {
    private static final ThreadLocal<Method> LOCAL = new ThreadLocal<>();

    private static final Counter.Builder SDK_OPERATIONS = Counter.builder("sdk.operations");
    private static final Counter.Builder SDK_OPERATIONS_SUCCESS = Counter.builder("sdk.operations.success");
    private static final Counter.Builder SDK_OPERATIONS_FAILTURE = Counter.builder("sdk.operations.failture");
    private static final Counter.Builder SDK_RETRY_ATTEMPS = Counter.builder("sdk.retry.attempts");

//    private static final Timer.Builder SDK_OPERATION_LATENCY = Timer.builder("sdk.operation.latency")
    private static final DistributionSummary.Builder SDK_OPERATION_LATENCY = DistributionSummary
            .builder("sdk.operation.latency")
            .serviceLevelObjectives(0.001, 0.002, 0.003, 0.004, 0.005, 0.0075, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1);

    public class Method {
        private final String name;

        private final LongAdder totalCount = new LongAdder();
        private final LongAdder totalTimeMs = new LongAdder();

        private final LongAdder count = new LongAdder();
        private final LongAdder timeMs = new LongAdder();

        private final Counter executionsCounter;
        private final Counter successCounter;
        private final Map<StatusCode, Counter> errorsCountersMap = new EnumMap<>(StatusCode.class);
        private final Map<StatusCode, Counter> retriesCountersMap = new EnumMap<>(StatusCode.class);
        private final Map<StatusCode, DistributionSummary> durationTimerMap = new EnumMap<>(StatusCode.class);
        private final Function<StatusCode, Counter> errorCounter;
        private final Function<StatusCode, Counter> retriesCounter;
        private final Function<StatusCode, DistributionSummary> durationTimer;

        private volatile long lastPrinted = 0;

        public Method(MeterRegistry registry, String name, String label) {
            this.name = name;
            this.executionsCounter = SDK_OPERATIONS.tag("operation_type", label).register(registry);
            this.successCounter = SDK_OPERATIONS_SUCCESS.tag("operation_type", label).register(registry);
            this.errorCounter = code -> errorsCountersMap.computeIfAbsent(code, key -> SDK_OPERATIONS_FAILTURE
                    .tag("operation_type", label)
                    .tag("operation_status", key.toString())
                    .register(registry)
            );
            this.retriesCounter = code -> retriesCountersMap.computeIfAbsent(code, key -> SDK_RETRY_ATTEMPS
                    .tag("operation_type", label)
                    .tag("operation_status", key.toString())
                    .register(registry)
            );
            this.durationTimer = code -> durationTimerMap.computeIfAbsent(code, key -> SDK_OPERATION_LATENCY
                    .tag("operation_type", label)
                    .tag("operation_status", key.toString())
                    .register(registry)
            );
        }

        public void measure(Runnable run) {
            LOCAL.set(this);

            executionsCounter.increment();

            StatusCode code = StatusCode.SUCCESS;
            long startedAt = System.currentTimeMillis();
            try {
                run.run();
                successCounter.increment();
            } catch (RuntimeException ex) {
                code = extractStatusCode(ex);
                errorCounter.apply(code).increment();
                throw ex;
            } finally {
                LOCAL.remove();

                long ms = System.currentTimeMillis() - startedAt;
                count.add(1);
                totalCount.add(1);
                timeMs.add(ms);
                totalTimeMs.add(ms);

                durationTimer.apply(code).record(0.001 * ms);
            }
        }

        public void close() {
            successCounter.close();
            executionsCounter.close();
            durationTimerMap.forEach((status, counter) -> counter.close());
            retriesCountersMap.forEach((status, counter) -> counter.close());
            errorsCountersMap.forEach((status, counter) -> counter.close());
        }

        private void reset() {
            count.reset();
            timeMs.reset();
            lastPrinted = System.currentTimeMillis();
        }

        private void print(Logger logger) {
            if (count.longValue() > 0 && lastPrinted != 0) {
                long ms = System.currentTimeMillis() - lastPrinted;
                double rps = 1000 * count.longValue() / ms;
                logger.info("{}\twas executed {} times\t with RPS {} ops", name, count.longValue(), rps);
            }

            reset();
        }

        private void printTotal(Logger logger) {
            if (totalCount.longValue() > 0) {
                double average = 1.0d  * totalTimeMs.longValue() / totalCount.longValue();
                logger.info("{}\twas executed {} times,\twith average time {} ms/op", name, totalCount.longValue(), average);
            }
        }
    }

    private final Logger logger;
    private final Method load;
    private final Method fetch;
    private final Method update;
    private final Method batchUpdate;

    private final AtomicInteger executionsCount = new AtomicInteger(0);
    private final AtomicInteger failturesCount = new AtomicInteger(0);
    private final AtomicInteger retriesCount = new AtomicInteger(0);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "ticker")
    );

    public AppMetrics(Logger logger, MeterRegistry meterRegistry) {
        this.logger = logger;
        this.load = new Method(meterRegistry, "LOAD  ", "load");
        this.fetch = new Method(meterRegistry, "FETCH ", "read");
        this.update = new Method(meterRegistry, "UPDATE", "update");
        this.batchUpdate = new Method(meterRegistry, "BULK_UP", "batch_update");
    }

    public Method getLoad() {
        return this.load;
    }

    public Method getFetch() {
        return this.fetch;
    }

    public Method getUpdate() {
        return this.update;
    }

    public Method getBatchUpdate() {
        return this.batchUpdate;
    }

    public void incrementFaiture() {
        failturesCount.incrementAndGet();
    }

    public void runWithMonitor(Runnable runnable) {
        Arrays.asList(load, fetch, update, batchUpdate).forEach(Method::reset);
        final ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(this::print, 1, 10, TimeUnit.SECONDS);
        runnable.run();
        future.cancel(false);
        print();
    }

    public void close() throws InterruptedException {
        scheduler.shutdownNow();
        scheduler.awaitTermination(20, TimeUnit.SECONDS);

        Arrays.asList(load, fetch, update, batchUpdate).forEach(m -> m.close());
    }

    private void print() {
        Arrays.asList(load, fetch, update, batchUpdate).forEach(m -> m.print(logger));
    }

    public void printTotal() {
        if (failturesCount.get() > 0) {
            logger.error("=========== TOTAL ==============");
            Arrays.asList(load, fetch, update, batchUpdate).forEach(m -> m.printTotal(logger));
            logger.error("Executed {} transactions with {} retries and {} failtures", executionsCount.get(),
                    retriesCount.get() - failturesCount.get(), failturesCount.get());
        } else {
            logger.info("=========== TOTAL ==============");
            Arrays.asList(load, fetch, update, batchUpdate).forEach(m -> m.printTotal(logger));
            logger.info("Executed {} transactions with {} retries", executionsCount.get(), retriesCount.get());
        }
    }

    public RetryListener getRetryListener() {
        return new RetryListener() {
            @Override
            public <T, E extends Throwable> boolean open(RetryContext ctx, RetryCallback<T, E> cb) {
                executionsCount.incrementAndGet();
                return true;
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext ctx, RetryCallback<T, E> cb, Throwable th) {
                logger.debug("Retry operation with error {} ", printSqlException(th));
                retriesCount.incrementAndGet();
                Method m = LOCAL.get();
                if (m != null) {
                    m.retriesCounter.apply(extractStatusCode(th)).increment();
                }
            }

            @Override
            public <T, E extends Throwable> void onSuccess(RetryContext context, RetryCallback<T, E> cb, T result) {
                // nothing
            }
        };
    }

    private String printSqlException(Throwable th) {
        Throwable ex = th;
        while (ex != null) {
            if (ex instanceof SQLException) {
                return ex.getMessage();
            }
            ex = ex.getCause();
        }
        return th.getMessage();
    }

    private StatusCode extractStatusCode(Throwable th) {
        Throwable ex = th;
        while (ex != null) {
            if (ex instanceof YdbStatusable) {
                return ((YdbStatusable) ex).getStatus().getCode();
            }
            ex = ex.getCause();
        }
        return StatusCode.CLIENT_INTERNAL_ERROR;
    }
}
