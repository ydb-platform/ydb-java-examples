package tech.ydb.slo.springjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import tech.ydb.jdbc.exception.YdbStatusable;
import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvSession;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.OpOutcome;
import tech.ydb.slo.core.kv.Row;
import tech.ydb.slo.core.kv.RowGenerator;

/**
 * {@link KvClient} backed by Spring Data JDBC ({@link JdbcTemplate}) and
 * {@code spring-ydb-retry}.
 */
@Component
public class SpringJdbcKvClient implements KvClient {
    private static final Logger logger = LoggerFactory.getLogger(SpringJdbcKvClient.class);

    private static final String CREATE_TABLE_QUERY_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `%s` ("
            + "  hash Uint64,"
            + "  id Uint64,"
            + "  payload_str Utf8,"
            + "  payload_double Double,"
            + "  payload_timestamp Timestamp,"
            + "  payload_hash Uint64,"
            + "  PRIMARY KEY (hash, id)"
            + ") WITH ("
            + "  UNIFORM_PARTITIONS = %d,"
            + "  AUTO_PARTITIONING_BY_SIZE = ENABLED,"
            + "  AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,"
            + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,"
            + "  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d"
            + ")";

    private static final String DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE `%s`";

    private final JdbcTemplate jdbc;
    private final KvOperationService operations;
    private String tablePath;

    public SpringJdbcKvClient(JdbcTemplate jdbc, KvOperationService operations) {
        this.jdbc = jdbc;
        this.operations = operations;
    }

    SpringJdbcKvClient forTable(String tablePath) {
        this.tablePath = tablePath;
        return this;
    }

    @Override
    public void createTable(KvWorkloadParams params, String tablePath) {
        jdbc.execute(String.format(
                CREATE_TABLE_QUERY_TEMPLATE,
                tablePath,
                params.minPartitionCount(),
                params.partitionSizeMb(),
                params.minPartitionCount(),
                params.maxPartitionCount()
        ));
    }

    @Override
    public void dropTable(String tablePath) {
        try {
            jdbc.execute(String.format(DROP_TABLE_QUERY_TEMPLATE, tablePath));
        } catch (RuntimeException e) {
            logger.warn("failed to drop table {}: {}", tablePath, e.toString());
        }
    }

    @Override
    public KvSession openSession() {
        return new SpringJdbcKvSession(operations, tablePath);
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
            try {
                long hash = RowGenerator.numericHash(id);
                operations.read(tablePath, id, hash);
                return OpOutcome.success(0);
            } catch (RuntimeException e) {
                return OpOutcome.error(0, classifyError(e));
            }
        }

        @Override
        public OpOutcome write(Row row, int timeoutMs) {
            try {
                long hash = RowGenerator.numericHash(row.id());
                operations.write(tablePath, row, hash);
                return OpOutcome.success(0);
            } catch (RuntimeException e) {
                return OpOutcome.error(0, classifyError(e));
            }
        }

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
            return e.getClass().getSimpleName().toLowerCase();
        }
    }
}
