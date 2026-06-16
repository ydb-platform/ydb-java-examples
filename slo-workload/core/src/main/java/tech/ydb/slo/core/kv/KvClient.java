package tech.ydb.slo.core.kv;

/**
 * The component-under-test adapter for the KV workload.
 *
 * <p>A {@code KvClient} is the only thing a concrete workload has to implement:
 * how to create / drop the table and how to open a {@link KvSession} that runs
 * reads and writes. Everything else — the load generator, rate limiting, the
 * prefill phase and the metric bookkeeping — lives in {@link WorkloadRunner}
 * and is shared across all workloads, so they all emit the same metric
 * contract.
 */
public interface KvClient extends AutoCloseable {

    /**
     * Creates the workload table (idempotently) using the partitioning
     * parameters from {@code params}.
     * @param params workload parameters
     * @param tablePath table path / name
     * @throws Exception if the table cannot be created
     */
    void createTable(KvWorkloadParams params, String tablePath) throws Exception;

    /**
     * Drops the workload table. Best-effort: called from a cleanup path, so
     * implementations should log rather than rethrow on failure.
     * @param tablePath table path / name
     */
    void dropTable(String tablePath);

    /**
     * Opens a session for a single worker thread. Called once per worker.
     * @return a new session
     * @throws Exception if the session cannot be opened
     */
    KvSession openSession() throws Exception;

    @Override
    default void close() throws Exception {
        // no shared resources to release by default
    }
}
