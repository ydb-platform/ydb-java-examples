package tech.ydb.apps;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PreDestroy;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.RetryListener;
import org.springframework.retry.annotation.EnableRetry;

import tech.ydb.apps.service.SchemeService;
import tech.ydb.apps.service.WorkloadService;
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

    private final AppMetrics ticker;

    private final Config config;
    private final SchemeService schemeService;
    private final WorkloadService workloadService;

    private final ExecutorService executor;
    private final AtomicInteger threadCounter = new AtomicInteger(0);
    private final AtomicLong logCounter = new AtomicLong(0);
    private volatile boolean isStopped = false;

    public Application(Config config, SchemeService scheme, WorkloadService worload, MeterRegistry registry) {
        GrpcMetrics.init(registry);

        this.config = config;
        this.schemeService = scheme;
        this.workloadService = worload;
        this.ticker = new AppMetrics(logger, registry);

        logger.info("Create fixed thread pool with size {}", config.getThreadCount());
        this.executor = Executors.newFixedThreadPool(config.getThreadCount(), this::threadFactory);
    }

    @PreDestroy
    public void close() throws Exception {
        isStopped = true;
        logger.info("CLI app is waiting for finishing");

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);

        ticker.printTotal();
        ticker.close();

        logger.info("CLI app has finished");
    }

    @Bean
    public RetryListener retryListener() {
        return ticker.getRetryListener();
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

            if ("validate".equalsIgnoreCase(arg)) {
                executeValidate();
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
                if (isStopped) {
                    return;
                }

                ticker.getLoad().measure(() -> {
                    workloadService.loadData(first, last);
                    logger.debug("inserted tokens [{}, {})", first, last);
                });
            }, executor));
        }

        futures.forEach(CompletableFuture::join);
    }

    private void test() {
        YdbTracer.current().markToPrint("test");

        int recordsCount = config.getRecordsCount();
        final Random rnd = new Random();
        Set<Integer> randomIds = IntStream.range(0, 100)
                .mapToObj(idx -> rnd.nextInt(recordsCount))
                .collect(Collectors.toSet());

        workloadService.updateBatch(randomIds, 0);
    }

    private void runWorkloads() {
        RateLimiter rt = config.getRpsLimiter();
        logCounter.set(workloadService.readLastGlobalVersion());
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

        while ((System.currentTimeMillis() < finishAt) && !isStopped) {
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
                ticker.incrementFaiture();
                logger.warn("got exception {}", ex.getMessage());
            }
        }
    }

    private void executeFetch(Random rnd, int recordCount) {
        int id = rnd.nextInt(recordCount);
        ticker.getFetch().measure(() -> workloadService.fetchToken(id));
    }

    private void executeUpdate(Random rnd, int recordCount) {
        int id = rnd.nextInt(recordCount);
        ticker.getUpdate().measure(() -> workloadService.updateToken(id, logCounter.incrementAndGet()));
    }

    private void executeBatchUpdate(Random rnd, int recordCount) {
        ticker.getBatchUpdate().measure(() -> {
            Set<Integer> randomIds = IntStream.range(0, 100)
                    .mapToObj(idx -> rnd.nextInt(recordCount))
                    .collect(Collectors.toSet());
            long counter = logCounter.getAndAdd(randomIds.size()) + 1;
            workloadService.updateBatch(randomIds, counter);
        });
    }

    private void executeValidate() {
        logger.info("=========== VALIDATE ==============");
        logger.info("Log table size      = {}", workloadService.countTokenLogs());
        logger.info("Last log version    = {}", workloadService.readLastGlobalVersion());
        logger.info("Token updates count = {}", workloadService.countAllTokenUpdates());
    }
}
