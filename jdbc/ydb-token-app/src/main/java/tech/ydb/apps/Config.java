package tech.ydb.apps;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.boot.context.properties.bind.Name;

/**
 *
 * @author Aleksandr Gorshenin
 */

@ConstructorBinding
@ConfigurationProperties(prefix = "app")
public class Config {
    private final String connection;
    private final int threadsCount;
    private final int recordsCount;
    private final int loadBatchSize;
    private final int workloadDurationSec;
    private final int rpsLimit;
    private final boolean otelEnabled;
    private final String otelEndpoint;
    private final String otelServiceName;

    public Config(
            String connection, int threadsCount, int recordsCount,
            @Name("load.batchSize") int loadBatchSize,
            @Name("workload.duration") int workloadDuration, int rpsLimit,
            @Name("otel.enabled") @DefaultValue("false") boolean otelEnabled,
            @Name("otel.endpoint") @DefaultValue("http://otel-collector:4317") String otelEndpoint,
            @Name("otel.serviceName") @DefaultValue("ydb-token-app") String otelServiceName) {
        this.connection = connection;
        this.threadsCount = threadsCount <= 0 ? Runtime.getRuntime().availableProcessors() : threadsCount;
        this.recordsCount = recordsCount;
        this.loadBatchSize = loadBatchSize;
        this.workloadDurationSec = workloadDuration;
        this.rpsLimit = rpsLimit;
        this.otelEnabled = otelEnabled;
        this.otelEndpoint = otelEndpoint;
        this.otelServiceName = otelServiceName;
    }

    public String getConnection() {
        return this.connection;
    }

    public int getThreadCount() {
        return this.threadsCount;
    }

    public int getRecordsCount() {
        return this.recordsCount;
    }

    public int getLoadBatchSize() {
        return this.loadBatchSize;
    }

    public int getWorkloadDurationSec() {
        return workloadDurationSec;
    }

    public RateLimiter getRpsLimiter() {
        return rpsLimit <= 0 ? RateLimiter.noLimit() : RateLimiter.withRps(rpsLimit);
    }

    public boolean isOtelEnabled() {
        return otelEnabled;
    }

    public String getOtelEndpoint() {
        return otelEndpoint;
    }

    public String getOtelServiceName() {
        return otelServiceName;
    }
}
