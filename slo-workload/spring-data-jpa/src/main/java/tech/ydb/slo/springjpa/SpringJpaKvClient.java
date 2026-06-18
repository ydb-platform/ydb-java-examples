package tech.ydb.slo.springjpa;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tech.ydb.jdbc.exception.YdbStatusable;
import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvSchema;
import tech.ydb.slo.core.kv.KvSession;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.OpOutcome;
import tech.ydb.slo.core.kv.Row;
import tech.ydb.slo.core.kv.RowGenerator;

@Component
public class SpringJpaKvClient {
    private final KvOperationService operations;

    public SpringJpaKvClient(KvOperationService operations) {
        this.operations = operations;
    }

    public KvClient forTable(String tablePath) {
        return new BoundClient(operations, tablePath);
    }

    private static final class BoundClient implements KvClient {
        private static final Logger logger = LoggerFactory.getLogger(BoundClient.class);

        private final KvOperationService operations;
        private final String tablePath;

        BoundClient(KvOperationService operations, String tablePath) {
            this.operations = operations;
            this.tablePath = tablePath;
        }

        @Override
        public void createTable(KvWorkloadParams params, String table) {
            operations.executeDdl(String.format(
                    KvSchema.CREATE_TABLE_TEMPLATE,
                    table,
                    params.minPartitionCount(),
                    params.partitionSizeMb(),
                    params.minPartitionCount(),
                    params.maxPartitionCount()
            ));
        }

        @Override
        public void dropTable(String table) {
            try {
                operations.executeDdl(String.format(KvSchema.DROP_TABLE_TEMPLATE, table));
            } catch (RuntimeException e) {
                logger.warn("failed to drop table {}: {}", table, e.toString());
            }
        }

        @Override
        public KvSession openSession() {
            return new SpringJpaKvSession(operations, tablePath);
        }

        @Override
        public void close() {
        }
    }

    private static final class SpringJpaKvSession implements KvSession {
        private final KvOperationService operations;
        private final String tablePath;

        private SpringJpaKvSession(KvOperationService operations, String tablePath) {
            this.operations = operations;
            this.tablePath = tablePath;
        }

        @Override
        public OpOutcome read(long id, int timeoutMs) {
            KvOperationService.resetAttempts();
            try {
                long hash = RowGenerator.numericHash(id);
                operations.read(tablePath, id, hash);
                return OpOutcome.success(Math.max(0, KvOperationService.currentAttempts() - 1));
            } catch (RuntimeException e) {
                return OpOutcome.error(
                        Math.max(0, KvOperationService.currentAttempts() - 1),
                        classifyError(e)
                );
            }
        }

        @Override
        public OpOutcome write(Row row, int timeoutMs) {
            KvOperationService.resetAttempts();
            try {
                long hash = RowGenerator.numericHash(row.id());
                operations.write(tablePath, row, hash);
                return OpOutcome.success(Math.max(0, KvOperationService.currentAttempts() - 1));
            } catch (RuntimeException e) {
                return OpOutcome.error(
                        Math.max(0, KvOperationService.currentAttempts() - 1),
                        classifyError(e)
                );
            }
        }

        private static String classifyError(Throwable e) {
            Throwable current = e;
            while (current != null) {
                if (current instanceof YdbStatusable) {
                    try {
                        return "ydb/" + ((YdbStatusable) current).getStatus().getCode().name().toLowerCase();
                    } catch (RuntimeException ignored) {
                    }
                }
                current = current.getCause();
            }
            current = e;
            while (current != null) {
                if (current instanceof SQLException) {
                    String state = ((SQLException) current).getSQLState();
                    if (state != null && !state.isEmpty()) {
                        return "sql/" + state;
                    }
                    return "sql/" + current.getClass().getSimpleName().toLowerCase();
                }
                current = current.getCause();
            }
            return e.getClass().getSimpleName().toLowerCase();
        }
    }
}
