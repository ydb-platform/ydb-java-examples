package tech.ydb.slo;

import java.time.Duration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.slo.core.Config;
import tech.ydb.slo.core.Launcher;
import tech.ydb.slo.core.Metrics;
import tech.ydb.slo.core.rmw.RmwRunner;
import tech.ydb.slo.queryrmw.QueryRmwClient;

public final class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final class Options {
        @Parameter(names = {"--duration", "--time"}, description = "Seconds; 0 means unlimited")
        private int duration = -1;

        @Parameter(names = "--writers", description = "Concurrent writers per bounded round (at least 2)")
        private int writers = 4;
    }

    private Main() { }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        Options options = new Options();
        Config config;
        try {
            JCommander.newBuilder().programName("query-rmw").acceptUnknownOptions(false)
                    .addObject(options).build().parse(args);
            config = Config.fromEnv("java-slo-query-rmw");
            if (options.duration < 0) {
                options.duration = config.durationSeconds();
            }
            if (options.duration < 0 || options.writers < 2) {
                throw new IllegalArgumentException("Require duration >= 0 and writers >= 2");
            }
        } catch (RuntimeException error) {
            logger.error("Invalid RMW configuration", error);
            return 2;
        }

        try (Metrics metrics = Metrics.create(config)) {
            String namespace = Launcher.tablePathFor(config);
            logger.info("Starting Query RMW: namespace={}, writers={}, duration={}s",
                    namespace, options.writers, options.duration);
            RmwRunner runner = new RmwRunner(new QueryRmwClient(config, namespace, metrics), options.writers);
            int code = runner.run(Duration.ofSeconds(options.duration));
            if (runner.failure() != null) {
                logger.error("Query RMW failed", runner.failure());
            }
            metrics.flush();
            return code;
        } catch (Throwable error) {
            logger.error("Query RMW failed", error);
            return 1;
        }
    }
}
