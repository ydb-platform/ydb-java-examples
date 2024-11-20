package tech.ydb.apps;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class Ticker {
    public class Measure implements AutoCloseable {
        private final Method method;
        private final long startedAt;
        private long count = 0;

        public Measure(Method method) {
            this.method = method;
            this.startedAt = System.currentTimeMillis();
        }

        public void inc() {
            count += 1;
        }

        @Override
        public void close() {
            if (count == 0) {
                return;
            }

            long ms = System.currentTimeMillis() - startedAt;

            method.count.add(count);
            method.totalCount.add(count);

            method.timeMs.add(ms);
            method.totalTimeMs.add(ms);
        }
    }

    public class Method {
        private final String name;

        private final LongAdder totalCount = new LongAdder();
        private final LongAdder totalTimeMs = new LongAdder();

        private final LongAdder count = new LongAdder();
        private final LongAdder timeMs = new LongAdder();

        private volatile long lastPrinted = 0;

        public Method(String name) {
            this.name = name;
        }

        public Measure newCall() {
            return new Measure(this);
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
    private final Method load = new Method("LOAD  ");
    private final Method fetch = new Method("FETCH ");
    private final Method update = new Method("UPDATE");
    private final Method batchUpdate = new Method("BULK_UP");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "ticker")
    );

    public Ticker(Logger logger) {
        this.logger = logger;
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
    }

    private void print() {
        Arrays.asList(load, fetch, update, batchUpdate).forEach(m -> m.print(logger));
    }

    public void printTotal() {
        logger.info("=========== TOTAL ==============");
        Arrays.asList(load, fetch, update, batchUpdate).forEach(m -> m.printTotal(logger));
    }
}
