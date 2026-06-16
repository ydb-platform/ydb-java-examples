package tech.ydb.slo.core.kv;

/**
 * A per-worker handle for executing KV operations.
 *
 * <p>The workload runner opens one session per worker thread and reuses it for
 * the whole loop, then closes it. This lets clients that need thread-confined
 * resources (e.g. the JDBC driver, whose connections are not thread-safe) keep
 * a connection and prepared statements alive for the worker's lifetime, while
 * clients that are inherently thread-safe (the native SDK retry context) can
 * simply share their state.
 *
 * <p>Implementations are responsible for their own retry handling and must
 * never throw from {@link #read} / {@link #write}: a failed operation is
 * reported as {@link OpOutcome#error}.
 */
public interface KvSession extends AutoCloseable {

    /**
     * Reads the row with the given id (its primary key is derived from the id).
     * @param id row id to read
     * @param timeoutMs per-attempt timeout in milliseconds
     * @return the operation outcome (never {@code null}, never throws)
     */
    OpOutcome read(long id, int timeoutMs);

    /**
     * Upserts a single row.
     * @param row row to write
     * @param timeoutMs per-attempt timeout in milliseconds
     * @return the operation outcome (never {@code null}, never throws)
     */
    OpOutcome write(Row row, int timeoutMs);

    @Override
    default void close() {
        // no resources to release by default
    }
}
