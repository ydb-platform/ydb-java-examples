package tech.ydb.slo.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
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
import tech.ydb.slo.core.kv.KvSchema;
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

    private static final long INITIAL_BACKOFF_MS = 10L;
    private static final long MAX_BACKOFF_MS = 1_000L;

    private final String jdbcUrl;
    private final Properties connectionProperties;
    private final String tablePath;
    private final int maxAttempts;

    public JdbcKvClient(Config config, KvWorkloadParams params, String tablePath) {
        this.jdbcUrl = config.jdbcUrl();
        this.tablePath = tablePath;
        this.maxAttempts = Math.max(1, params.maxAttempts());
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
                    KvSchema.CREATE_TABLE_TEMPLATE,
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
            stmt.execute(String.format(KvSchema.DROP_TABLE_TEMPLATE, tablePath));
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
            while (attempts < maxAttempts) {
                attempts++;
                try {
                    read(id, hash, timeoutSeconds(timeoutMs));
                    return OpOutcome.success(attempts - 1);
                } catch (SQLException e) {
                    last = e;
                    if (!isRetryable(e) || attempts >= maxAttempts) {
                        break;
                    }
                    invalidateOnConnectionError(e);
                    if (!backoff(attempts)) {
                        // Interrupt during shutdown: stop retrying, return the
                        // last error so the worker loop can exit cleanly without
                        // contributing a spurious "interrupted" entry to errors.
                        break;
                    }
                }
            }
            return OpOutcome.error(attempts - 1, classifyError(last));
        }

        @Override
        public OpOutcome write(Row row, int timeoutMs) {
            long hash = RowGenerator.numericHash(row.id());
            int attempts = 0;
            SQLException last = null;
            while (attempts < maxAttempts) {
                attempts++;
                try {
                    write(row, hash, timeoutSeconds(timeoutMs));
                    return OpOutcome.success(attempts - 1);
                } catch (SQLException e) {
                    last = e;
                    if (!isRetryable(e) || attempts >= maxAttempts) {
                        break;
                    }
                    invalidateOnConnectionError(e);
                    if (!backoff(attempts)) {
                        break;
                    }
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
                readStmt = conn.prepareStatement(String.format(KvSchema.SELECT_TEMPLATE, tablePath));
            }
            return readStmt;
        }

        private PreparedStatement writeStmt() throws SQLException {
            Connection conn = connection();
            if (writeStmt == null) {
                writeStmt = conn.prepareStatement(String.format(KvSchema.UPSERT_TEMPLATE, tablePath));
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

    /*
     * Reads and writes are idempotent (SELECT and UPSERT respectively), so any
     * YdbStatusable error whose status code is retryable for idempotent ops
     * should be retried. The plain JDBC exception types are kept as a fallback
     * for non-YDB errors (e.g. a network blip surfaced as a generic
     * SQLRecoverableException).
     */
    private static boolean isRetryable(SQLException e) {
        if (e instanceof YdbStatusable) {
            try {
                return ((YdbStatusable) e).getStatus().getCode().isRetryable(true);
            } catch (RuntimeException ignored) {
                // fall through
            }
        }
        return e instanceof SQLRecoverableException || e instanceof SQLTransientException;
    }

    private static boolean isConnectionError(SQLException e) {
        return e instanceof SQLRecoverableException
                || e instanceof SQLTransientConnectionException
                || e instanceof SQLNonTransientConnectionException;
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

    /**
     * Sleeps for an exponentially growing delay between retry attempts.
     * @return {@code true} if the sleep completed normally, {@code false} if
     *     the thread was interrupted (caller should stop retrying)
     */
    private static boolean backoff(int attempt) {
        long delay = Math.min(MAX_BACKOFF_MS, INITIAL_BACKOFF_MS * (1L << Math.min(attempt - 1, 20)));
        try {
            Thread.sleep(delay);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
