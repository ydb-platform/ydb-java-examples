package tech.ydb.apps;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
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

    public Config(String connection, int threadsCount, int recordsCount, @Name("load.batchSize") int loadBatchSize,
            @Name("workload.duration") int workloadDuration, int rpsLimit) {
        this.connection = connection;
        this.threadsCount = threadsCount <= 0 ? Runtime.getRuntime().availableProcessors() : threadsCount;
        this.recordsCount = recordsCount;
        this.loadBatchSize = loadBatchSize;
        this.workloadDurationSec = workloadDuration;
        this.rpsLimit = rpsLimit;
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

}
