package tech.ydb.slo.kv;

import com.beust.jcommander.Parameter;

/**
 * Tunable parameters for the KV workload.
 *
 * <p>Defaults match the SLO workloads in the Go and JavaScript SDKs so the
 * three runs are comparable. JCommander annotations let the operator override
 * any field from the command line, e.g.
 * {@code --read-rps 500 --write-rps 50}.
 */
public final class KvWorkloadParams {

    @Parameter(
            names = {"--read-rps"},
            description = "Target read operations per second"
    )
    private int readRps = 1000;

    @Parameter(
            names = {"--write-rps"},
            description = "Target write operations per second"
    )
    private int writeRps = 100;

    @Parameter(
            names = {"--read-timeout-ms"},
            description = "Per-attempt read timeout in milliseconds"
    )
    private int readTimeoutMs = 10_000;

    @Parameter(
            names = {"--write-timeout-ms"},
            description = "Per-attempt write timeout in milliseconds"
    )
    private int writeTimeoutMs = 10_000;

    @Parameter(
            names = {"--prefill-count"},
            description = "Number of rows to prefill before the run phase"
    )
    private long prefillCount = 1_000L;

    @Parameter(
            names = {"--partition-size"},
            description = "Auto-partitioning partition size in MB"
    )
    private int partitionSizeMb = 1;

    @Parameter(
            names = {"--min-partition-count"},
            description = "Minimum number of table partitions"
    )
    private int minPartitionCount = 6;

    @Parameter(
            names = {"--max-partition-count"},
            description = "Maximum number of table partitions"
    )
    private int maxPartitionCount = 1_000;

    @Parameter(
            names = {"--duration"},
            description = "Run duration in seconds (overrides WORKLOAD_DURATION when > 0)"
    )
    private int durationSeconds = 0;

    public int readRps() {
        return readRps;
    }

    public int writeRps() {
        return writeRps;
    }

    public int readTimeoutMs() {
        return readTimeoutMs;
    }

    public int writeTimeoutMs() {
        return writeTimeoutMs;
    }

    public long prefillCount() {
        return prefillCount;
    }

    public int partitionSizeMb() {
        return partitionSizeMb;
    }

    public int minPartitionCount() {
        return minPartitionCount;
    }

    public int maxPartitionCount() {
        return maxPartitionCount;
    }

    /**
     * Effective run duration. If the CLI flag was omitted (left at 0), falls
     * back to the value supplied via the {@code WORKLOAD_DURATION} environment
     * variable through {@code Config}.
     */
    public int durationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(int durationSeconds) {
        this.durationSeconds = durationSeconds;
    }
}
