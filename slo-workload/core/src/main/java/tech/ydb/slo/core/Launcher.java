package tech.ydb.slo.core;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.WorkloadRunner;

public final class Launcher {
    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);



    @FunctionalInterface
    public interface ClientFactory {
        KvClient create(Config config, KvWorkloadParams params, String tablePath) throws Exception;
    }

    private Launcher() {

    }



    public static void launch(
            String programName,
            String defaultWorkloadName,
            String[] args,
            ClientFactory factory
    ) {
        System.exit(run(programName, defaultWorkloadName, args, factory));
    }



    public static int run(
            String programName,
            String defaultWorkloadName,
            String[] args,
            ClientFactory factory
    ) {
        Config config;
        try {
            config = Config.fromEnv(defaultWorkloadName);
        } catch (IllegalStateException e) {
            logger.error("invalid environment configuration: {}", e.getMessage());
            return 2;
        }

        KvWorkloadParams params = new KvWorkloadParams();
        try {
            JCommander.newBuilder()
                    .programName(programName)


                    .acceptUnknownOptions(false)
                    .addObject(params)
                    .build()
                    .parse(args);
        } catch (ParameterException e) {
            logger.error("invalid CLI arguments: {}", e.getMessage());
            return 2;
        }


        if (params.durationSeconds() <= 0) {
            params.setDurationSeconds(config.durationSeconds());
        }

        String tablePath = tablePathFor(config);

        logger.info("starting SLO workload: name={}, ref={}, duration={}s, readRps={}, writeRps={}, table={}",
                config.workloadName(),
                config.ref(),
                params.durationSeconds(),
                params.readRps(),
                params.writeRps(),
                tablePath);

        int exitCode = 0;
        Metrics metrics = Metrics.create(config);
        KvClient client;
        try {
            client = factory.create(config, params, tablePath);
        } catch (Exception e) {
            logger.error("failed to create workload client", e);
            closeQuietly(metrics, "metrics");
            return 1;
        }

        WorkloadRunner runner = new WorkloadRunner(client, metrics, params, tablePath);
        try {
            runner.setup();
            runner.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("workload interrupted");
            exitCode = 1;
        } catch (Throwable t) {
            logger.error("workload failed", t);
            exitCode = 1;
        } finally {
            try {
                runner.teardown();
            } catch (Throwable t) {
                logger.warn("teardown failed", t);
            }
            try {
                metrics.flush();
            } catch (Throwable t) {
                logger.warn("metrics flush failed", t);
            }
            closeQuietly(metrics, "metrics");
            closeQuietly(client, "workload client");
        }

        return exitCode;
    }



    public static String tablePathFor(Config config) {
        return sanitize(config.workloadName()) + "_" + sanitize(config.ref());
    }

    private static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Throwable t) {
            logger.warn("failed to close {}: {}", name, t.toString());
        }
    }



    private static String sanitize(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        if (sb.length() == 0) {
            sb.append('_');
        } else if (Character.isDigit(sb.charAt(0))) {
            sb.insert(0, '_');
        }

        if (sb.length() > 64) {
            sb.setLength(64);
        }
        return sb.toString();
    }
}
