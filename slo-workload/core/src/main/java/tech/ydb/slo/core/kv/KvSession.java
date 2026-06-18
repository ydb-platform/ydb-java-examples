package tech.ydb.slo.core.kv;

public interface KvSession extends AutoCloseable {
    OpOutcome read(long id, int timeoutMs);

    OpOutcome write(Row row, int timeoutMs);

    @Override
    default void close() {
    }
}
