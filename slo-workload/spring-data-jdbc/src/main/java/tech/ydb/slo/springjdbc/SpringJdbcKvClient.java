package tech.ydb.slo.springjdbc;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import tech.ydb.jdbc.exception.YdbStatusable;
import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvSchema;
import tech.ydb.slo.core.kv.KvSession;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.OpOutcome;
import tech.ydb.slo.core.kv.Row;
import tech.ydb.slo.core.kv.RowGenerator;

/**
 * {@link KvClient} backed by Spring's {@link JdbcTemplate} and
 * {@code spring-ydb-retry}.
 *
 * <p>This singleton bean is bound to a workload table by calling
 * {@link #forTable(String)}, which returns a fresh {@link KvClient} that closes
 * over the table path — the bean itself stays immutable so Spring's
 * thread-safety contract is preserved.
 */
@Component
public class SpringJdbcKvClient {
    private final JdbcTemplate jdbc;
    private final KvOperationService operations;

    public SpringJdbcKvClient(JdbcTemplate jdbc, KvOperationService operations) {
        this.jdbc = jdbc;
        this.operations = operations;
    }

    /**
     * Binds the workload to a table path, returning a {@link KvClient} that
     * proxies create/drop/openSession against it. The returned client shares
     * the singleton's {@code JdbcTemplate} and {@code KvOperationService}, so
     * Hikari pooling + AOP retry stay in effect.
     */
    public KvClient forTable(String tablePath) {
        return new BoundClient(jdbc, operations, tablePath);
    }

    private static final class BoundClient implements KvClient {
        private static final Logger logger = LoggerFactory.getLogger(BoundClient.class);

        private final JdbcTemplate jdbc;
        private final KvOperationService operations;
        private final String tablePath;

        BoundClient(JdbcTemplate jdbc, KvOperationService operations, String tablePath) {
            this.jdbc = jdbc;
            this.operations = operations;
            this.tablePath = tablePath;
        }

        @Override
        public void createTable(KvWorkloadParams params, String table) {
            jdbc.execute(String.format(
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
                jdbc.execute(String.format(KvSchema.DROP_TABLE_TEMPLATE, table));
            } catch (RuntimeException e) {
                logger.warn("failed to drop table {}: {}", table, e.toString());
            }
        }

        @Override
        public KvSession openSession() {
            return new SpringJdbcKvSession(operations, tablePath);
        }
    }

    private static final class SpringJdbcKvSession implements KvSession {
        private final KvOperationService operations;
        private final String tablePath;

        private SpringJdbcKvSession(KvOperationService operations, String tablePath) {
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

        /*
         * Walks the cause chain looking for the most informative label:
         *   - a YDB status (best),
         *   - else a SQLState (preserves Hikari pool-exhaustion → 08006, etc.),
         *   - else the originating exception's simple name.
         * Without the SQLException step, Hikari's CannotGetJdbcConnectionException
         * masks the underlying transient state and every chaos run collapses to
         * a single error_kind bucket.
         */
        private static String classifyError(Throwable e) {
            Throwable current = e;
            while (current != null) {
                if (current instanceof YdbStatusable) {
                    try {
                        return "ydb/" + ((YdbStatusable) current).getStatus().getCode().name().toLowerCase();
                    } catch (RuntimeException ignored) {
                        // fall through
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
