package tech.ydb.slo.core;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.WorkloadRunner;

/**
 * Shared entry-point glue for the standalone (non-Spring) SLO workloads.
 *
 * <p>Reads connection details and run parameters from environment variables
 * (see {@link Config}), parses workload-specific flags from the command line
 * (see {@link KvWorkloadParams}), builds the workload's {@link KvClient} via
 * the supplied factory, and runs the KV workload phases — setup, run, teardown
 * — pushing metrics to the OTLP endpoint configured by the YDB SLO action
 * runtime.
 *
 * <p>Exit codes:
 * <ul>
 *   <li>{@code 0} — workload completed successfully</li>
 *   <li>{@code 1} — workload failed (an unhandled exception or interrupted run)</li>
 *   <li>{@code 2} — invalid CLI arguments or environment configuration</li>
 * </ul>
 */
public final class Launcher {
    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    /**
     * Builds the component-under-test client once the connection details and
     * parameters are known.
     */
    @FunctionalInterface
    public interface ClientFactory {
        KvClient create(Config config, KvWorkloadParams params, String tablePath) throws Exception;
    }

    private Launcher() {
        // utility class
    }

    /**
     * Loads configuration, parses arguments, runs the workload and terminates
     * the JVM with the appropriate exit code.
     *
     * @param programName name shown in CLI usage / logs
     * @param defaultWorkloadName workload name used when {@code WORKLOAD_NAME} is unset
     * @param args raw command-line arguments
     * @param factory builds the workload's {@link KvClient}
     */
    public static void launch(
            String programName,
            String defaultWorkloadName,
            String[] args,
            ClientFactory factory
    ) {
        System.exit(run(programName, defaultWorkloadName, args, factory));
    }

    /**
     * Same as {@link #launch} but returns the exit code instead of calling
     * {@link System#exit}, so callers (e.g. Spring runners) can manage the
     * lifecycle themselves.
     * @param programName name shown in CLI usage / logs
     * @param defaultWorkloadName workload name used when {@code WORKLOAD_NAME} is unset
     * @param args raw command-line arguments
     * @param factory builds the workload's {@link KvClient}
     * @return process exit code
     */
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
                    .acceptUnknownOptions(true)
                    .addObject(params)
                    .build()
                    .parse(args);
        } catch (ParameterException e) {
            logger.error("invalid CLI arguments: {}", e.getMessage());
            return 2;
        }

        // CLI duration takes precedence over WORKLOAD_DURATION when supplied.
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

    /**
     * Builds the workload table path. It embeds the workload name and ref so
     * concurrent runs of the current and baseline images don't step on each
     * other. Both components are sanitized: {@code WORKLOAD_NAME} comes from
     * the action input and is normally already safe, but user input is not
     * trusted to be a valid YDB identifier.
     * @param config workload configuration
     * @return sanitized table path
     */
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

    /*
     * Replaces characters that aren't valid in YDB table names with
     * underscores. Refs from CI may include slashes (release/1.2) or dots,
     * which the action permits in metrics labels but YDB rejects in table
     * paths.
     */
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
        return sb.toString();
    }
}
