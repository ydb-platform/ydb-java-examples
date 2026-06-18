package tech.ydb.slo.core.kv;

public interface KvClient extends AutoCloseable {
    void createTable(KvWorkloadParams params, String tablePath) throws Exception;

    void dropTable(String tablePath);

    KvSession openSession() throws Exception;

    @Override
    default void close() {
    }
}
