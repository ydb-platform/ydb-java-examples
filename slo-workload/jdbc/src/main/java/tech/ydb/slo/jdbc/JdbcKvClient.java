package tech.ydb.slo.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.jdbc.exception.YdbStatusable;
import tech.ydb.slo.core.Config;
import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvSession;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.OpOutcome;
import tech.ydb.slo.core.kv.Row;
import tech.ydb.slo.core.kv.RowGenerator;

/**
 * {@link KvClient} backed by the YDB JDBC driver with application-level retry.
 */
public final class JdbcKvClient implements KvClient {
    private static final Logger logger = LoggerFactory.getLogger(JdbcKvClient.class);

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

    private static final String WRITE_QUERY_TEMPLATE = ""
            + "UPSERT INTO `%s` ("
            + "  hash, id, payload_str, payload_double, payload_timestamp, payload_hash"
            + ") VALUES (?, ?, ?, ?, ?, ?)";

    private static final String READ_QUERY_TEMPLATE = ""
            + "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
            + "  FROM `%s`"
            + "  WHERE id = ? AND hash = ?";

    private static final int MAX_ATTEMPTS = 10;
    private static final long INITIAL_BACKOFF_MS = 10L;
    private static final long MAX_BACKOFF_MS = 1_000L;

    private final String jdbcUrl;
    private final Properties connectionProperties;
    private final String tablePath;

    public JdbcKvClient(Config config, String tablePath) {
        this.jdbcUrl = config.jdbcUrl();
        this.tablePath = tablePath;
        this.connectionProperties = new Properties();
        if (config.token() != null && !config.token().isEmpty()) {
            connectionProperties.setProperty("token", config.token());
        }
    }

    @Override
    public void createTable(KvWorkloadParams params, String tablePath) throws SQLException {
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    CREATE_TABLE_QUERY_TEMPLATE,
                    tablePath,
                    params.minPartitionCount(),
                    params.partitionSizeMb(),
                    params.minPartitionCount(),
                    params.maxPartitionCount()
            ));
        }
    }

    @Override
    public void dropTable(String tablePath) {
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(DROP_TABLE_QUERY_TEMPLATE, tablePath));
        } catch (SQLException e) {
            logger.warn("failed to drop table {}: {}", tablePath, e.toString());
        }
    }

    @Override
    public KvSession openSession() throws SQLException {
        return new JdbcKvSession();
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, connectionProperties);
    }

    private final class JdbcKvSession implements KvSession {
        private Connection connection;
        private PreparedStatement readStmt;
        private PreparedStatement writeStmt;

        @Override
        public OpOutcome read(long id, int timeoutMs) {
            long hash = RowGenerator.numericHash(id);
            int attempts = 0;
            SQLException last = null;
            while (attempts < MAX_ATTEMPTS) {
                attempts++;
                try {
                    read(id, hash, timeoutSeconds(timeoutMs));
                    return OpOutcome.success(attempts - 1);
                } catch (SQLException e) {
                    last = e;
                    if (!isRetryable(e) || attempts >= MAX_ATTEMPTS) {
                        break;
                    }
                    invalidateOnConnectionError(e);
                    backoff(attempts);
                }
            }
            return OpOutcome.error(attempts - 1, classifyError(last));
        }

        @Override
        public OpOutcome write(Row row, int timeoutMs) {
            long hash = RowGenerator.numericHash(row.id());
            int attempts = 0;
            SQLException last = null;
            while (attempts < MAX_ATTEMPTS) {
                attempts++;
                try {
                    write(row, hash, timeoutSeconds(timeoutMs));
                    return OpOutcome.success(attempts - 1);
                } catch (SQLException e) {
                    last = e;
                    if (!isRetryable(e) || attempts >= MAX_ATTEMPTS) {
                        break;
                    }
                    invalidateOnConnectionError(e);
                    backoff(attempts);
                }
            }
            return OpOutcome.error(attempts - 1, classifyError(last));
        }

        @Override
        public void close() {
            closeQuietly(readStmt);
            closeQuietly(writeStmt);
            closeQuietly(connection);
            readStmt = null;
            writeStmt = null;
            connection = null;
        }

        private Connection connection() throws SQLException {
            if (connection == null || connection.isClosed()) {
                connection = openConnection();
                readStmt = null;
                writeStmt = null;
            }
            return connection;
        }

        private PreparedStatement readStmt() throws SQLException {
            Connection conn = connection();
            if (readStmt == null) {
                readStmt = conn.prepareStatement(String.format(READ_QUERY_TEMPLATE, tablePath));
            }
            return readStmt;
        }

        private PreparedStatement writeStmt() throws SQLException {
            Connection conn = connection();
            if (writeStmt == null) {
                writeStmt = conn.prepareStatement(String.format(WRITE_QUERY_TEMPLATE, tablePath));
            }
            return writeStmt;
        }

        private void read(long id, long hash, int timeoutSeconds) throws SQLException {
            PreparedStatement stmt = readStmt();
            stmt.setQueryTimeout(timeoutSeconds);
            stmt.setLong(1, id);
            stmt.setLong(2, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    rs.getLong("id");
                }
            }
        }

        private void write(Row row, long hash, int timeoutSeconds) throws SQLException {
            PreparedStatement stmt = writeStmt();
            stmt.setQueryTimeout(timeoutSeconds);
            stmt.setLong(1, hash);
            stmt.setLong(2, row.id());
            stmt.setString(3, row.payloadStr());
            stmt.setDouble(4, row.payloadDouble());
            stmt.setTimestamp(5, Timestamp.from(row.payloadTimestamp()));
            stmt.setLong(6, row.payloadHash());
            stmt.executeUpdate();
        }

        private void invalidateOnConnectionError(SQLException e) {
            if (isConnectionError(e)) {
                close();
            }
        }
    }

    private static int timeoutSeconds(int timeoutMs) {
        return Math.max(1, (timeoutMs + 999) / 1000);
    }

    private static boolean isRetryable(SQLException e) {
        return e instanceof SQLRecoverableException || e instanceof SQLTransientException;
    }

    private static boolean isConnectionError(SQLException e) {
        return e instanceof SQLRecoverableException || e instanceof SQLTransientConnectionException;
    }

    private static String classifyError(SQLException e) {
        if (e == null) {
            return "unknown";
        }
        if (e instanceof YdbStatusable) {
            try {
                return "ydb/" + ((YdbStatusable) e).getStatus().getCode().name().toLowerCase();
            } catch (RuntimeException ignored) {
                // fall through
            }
        }
        return e.getClass().getSimpleName().toLowerCase();
    }

    private static void backoff(int attempt) {
        long delay = Math.min(MAX_BACKOFF_MS, INITIAL_BACKOFF_MS * (1L << Math.min(attempt - 1, 20)));
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
