package tech.ydb.apps;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.annotation.EnableRetry;

import tech.ydb.apps.service.SchemeService;
import tech.ydb.apps.service.TokenService;
import tech.ydb.jdbc.YdbTracer;

/**
 *
 * @author Aleksandr Gorshenin
 */
@EnableRetry
@EnableConfigurationProperties(Config.class)
@SpringBootApplication
public class Application implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        try {
            SpringApplication.run(Application.class, args).close();
        } catch (Exception ex) {
            logger.error("App finished with error", ex);
        }
    }

    private final Ticker ticker = new Ticker(logger);

    private final Config config;
    private final SchemeService schemeService;
    private final TokenService tokenService;

    private final ExecutorService executor;
    private final AtomicInteger threadCounter = new AtomicInteger(0);
    private final AtomicInteger executionsCount = new AtomicInteger(0);
    private final AtomicInteger retriesCount = new AtomicInteger(0);

    public Application(Config config, SchemeService schemeService, TokenService tokenService) {
        this.config = config;
        this.schemeService = schemeService;
        this.tokenService = tokenService;

        this.executor = Executors.newFixedThreadPool(config.getThreadCount(), this::threadFactory);
    }

    @PreDestroy
    public void close() throws Exception {
        logger.info("CLI app is waiting for finishing");

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);

        ticker.printTotal();
        ticker.close();

        logger.info("Executed {} transactions with {} retries", executionsCount.get(), retriesCount.get());
        logger.info("CLI app has finished");
    }

    @Bean
    public RetryListener retryListener() {
        return new RetryListener() {
            @Override
            public <T, E extends Throwable> boolean open(RetryContext ctx, RetryCallback<T, E> callback) {
                executionsCount.incrementAndGet();
                return true;
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext ctx, RetryCallback<T, E> callback, Throwable th) {
                logger.debug("Retry operation with error {} ", printSqlException(th));
                retriesCount.incrementAndGet();
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

    @Override
    public void run(String... args) {
        logger.info("CLI app has started with database {}", config.getConnection());

        for (String arg : args) {
            if (arg.startsWith("--")) { // skip Spring parameters
                continue;
            }

            logger.info("execute {} step", arg);

            if ("clean".equalsIgnoreCase(arg)) {
                schemeService.executeClean();
            }

            if ("init".equalsIgnoreCase(arg)) {
                schemeService.executeInit();
            }

            if ("load".equalsIgnoreCase(arg)) {
                ticker.runWithMonitor(this::loadData);
            }

            if ("run".equalsIgnoreCase(arg)) {
                ticker.runWithMonitor(this::runWorkloads);
            }

            if ("test".equalsIgnoreCase(arg)) {
                ticker.runWithMonitor(this::test);
            }
        }
    }

    private Thread threadFactory(Runnable runnable) {
        return new Thread(runnable, "app-thread-" + threadCounter.incrementAndGet());
    }

    private void loadData() {
        int recordsCount = config.getRecordsCount();
        int batchSize = config.getLoadBatchSize();

        List<CompletableFuture<?>> futures = new ArrayList<>();
        int id = 0;
        while (id < recordsCount) {
            final int first = id;
            id += batchSize;
            final int last = id < recordsCount ? id : recordsCount;

            futures.add(CompletableFuture.runAsync(() -> {
                try (Ticker.Measure measure = ticker.getLoad().newCall()) {
                    tokenService.insertBatch(first, last);
                    logger.debug("inserted tokens [{}, {})", first, last);
                    measure.inc();
                }
            }, executor));
        }

        futures.forEach(CompletableFuture::join);
    }

    private void test() {
        YdbTracer.current().markToPrint("test");

        int recordsCount = config.getRecordsCount();
        final Random rnd = new Random();
        List<Integer> randomIds = IntStream.range(0, 100)
                .mapToObj(idx -> rnd.nextInt(recordsCount))
                .collect(Collectors.toList());

        tokenService.updateBatch(randomIds);
    }

    private void runWorkloads() {
        RateLimiter rt = config.getRpsLimiter();
        long finishAt = System.currentTimeMillis() + config.getWorkloadDurationSec() * 1000;
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < config.getThreadCount(); i++) {
            futures.add(CompletableFuture.runAsync(() -> this.workload(rt, finishAt), executor));
        }

        futures.forEach(CompletableFuture::join);
    }

    private void workload(RateLimiter rt, long finishAt) {
        final Random rnd = new Random();
        final int recordCount = config.getRecordsCount();

        while (System.currentTimeMillis() < finishAt) {
            rt.acquire();
            int mode = rnd.nextInt(10);

            try {
                if (mode < 5) {
                    executeFetch(rnd, recordCount);  // 50 percents
                } else if (mode < 9) {
                    executeUpdate(rnd, recordCount); // 40 percents
                } else {
                    executeBatchUpdate(rnd, recordCount); // 10 percents
                }
            } catch (RuntimeException ex) {
                logger.debug("got exception {}", ex.getMessage());
            }
        }
    }

    private void executeFetch(Random rnd, int recordCount) {
        int id = rnd.nextInt(recordCount);
        try (Ticker.Measure measure = ticker.getFetch().newCall()) {
            tokenService.fetchToken(id);
            measure.inc();
        }
    }

    private void executeUpdate(Random rnd, int recordCount) {
        int id = rnd.nextInt(recordCount);
        try (Ticker.Measure measure = ticker.getUpdate().newCall()) {
            tokenService.updateToken(id);
            measure.inc();
        }
    }

    private void executeBatchUpdate(Random rnd, int recordCount) {
        try (Ticker.Measure measure = ticker.getBatchUpdate().newCall()) {
            List<Integer> randomIds = IntStream.range(0, 100)
                    .mapToObj(idx -> rnd.nextInt(recordCount))
                    .collect(Collectors.toList());
            tokenService.updateBatch(randomIds);
            measure.inc();
        }
    }
}
