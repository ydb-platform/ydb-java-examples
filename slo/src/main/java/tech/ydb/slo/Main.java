package tech.ydb.slo;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.slo.kv.KvWorkload;
import tech.ydb.slo.kv.KvWorkloadParams;

/**
 * Entry point of the SLO workload.
 *
 * <p>Reads connection details and run parameters from environment variables
 * (see {@link Config}), parses workload-specific flags from the command line
 * (see {@link KvWorkloadParams}), and runs the KV workload phases — setup,
 * run, teardown — pushing metrics to the OTLP endpoint configured by the YDB
 * SLO action runtime.
 *
 * <p>Exit codes:
 * <ul>
 *   <li>{@code 0} — workload completed successfully</li>
 *   <li>{@code 1} — workload failed (an unhandled exception or interrupted run)</li>
 *   <li>{@code 2} — invalid CLI arguments</li>
 * </ul>
 */
public final class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private Main() {
        // utility class
    }

    public static void main(String[] args) {
        Config config;
        try {
            config = Config.fromEnv();
        } catch (IllegalStateException e) {
            logger.error("invalid environment configuration: {}", e.getMessage());
            System.exit(2);
            return;
        }

        KvWorkloadParams params = new KvWorkloadParams();
        try {
            JCommander.newBuilder()
                    .programName("ydb-slo-workload")
                    .acceptUnknownOptions(true)
                    .addObject(params)
                    .build()
                    .parse(args);
        } catch (ParameterException e) {
            logger.error("invalid CLI arguments: {}", e.getMessage());
            System.exit(2);
            return;
        }

        // CLI duration takes precedence over WORKLOAD_DURATION when supplied.
        if (params.durationSeconds() <= 0) {
            params.setDurationSeconds(config.durationSeconds());
        }

        logger.info("starting SLO workload: name={}, ref={}, duration={}s, readRps={}, writeRps={}",
                config.workloadName(),
                config.ref(),
                params.durationSeconds(),
                params.readRps(),
                params.writeRps());

        // The table path embeds workload name and ref so concurrent runs of
        // the current and baseline images don't step on each other. Both
        // components are sanitized: WORKLOAD_NAME comes from the action input
        // and is normally already safe, but we don't trust user input to be
        // a valid YDB identifier.
        String tablePath = sanitize(config.workloadName()) + "_" + sanitize(config.ref());

        int exitCode = 0;
        Metrics metrics = Metrics.create(config);
        AuthProvider provider = NopAuthProvider.INSTANCE;
        if (config.token() != null && !config.token().isEmpty()) {
            provider = new TokenAuthProvider(config.token());
        }
        GrpcTransport transport = GrpcTransport.forConnectionString(config.connectionString())
                .withAuthProvider(provider)
                .build();
        QueryClient queryClient = QueryClient.newClient(transport).build();

        KvWorkload workload = new KvWorkload(queryClient, metrics, params, tablePath);

        try {
            workload.setup();
            workload.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("workload interrupted");
            exitCode = 1;
        } catch (Throwable t) {
            logger.error("workload failed", t);
            exitCode = 1;
        } finally {
            try {
                workload.teardown();
            } catch (Throwable t) {
                logger.warn("teardown failed", t);
            }

            try {
                metrics.flush();
            } catch (Throwable t) {
                logger.warn("metrics flush failed", t);
            }

            closeQuietly(metrics, "metrics");
            closeQuietly(queryClient, "query client");
            closeQuietly(transport, "transport");
        }

        System.exit(exitCode);
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

    /**
     * Replaces characters that aren't valid in YDB table names with underscores.
     * Refs from CI may include slashes ({@code release/1.2}) or dots, which
     * the action permits in metrics labels but YDB rejects in table paths.
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
