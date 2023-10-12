package tech.ydb.jdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.jdbc.exception.YdbExecutionStatusException;
import tech.ydb.jdbc.exception.YdbRetryableException;

public class Main {
    private final static Logger LOG = LoggerFactory.getLogger(Main.class);
    private final static String TABLE_NAME = "jdbc_batch_example";

    private final static String CREATE_TABLE_SQL = ""
            + "CREATE TABLE " + TABLE_NAME + "("
            + "  app Text,"
            + "  timestamp Timestamp,"
            + "  host Text,"
            + "  http_code UInt32,"
            + "  message Text,"
            + "  PRIMARY KEY(app, timestamp, host)"
            + ")";

    private final static String BATCH_UPSERT_SQL = ""
            + "DECLARE $values AS List<Struct<p1:Text, p2:Timestamp, p3:Text, p4:UInt32, p5:Text>>; "
            + "$mapper = ($r) -> (AsStruct("
            + "  $r.p1 as app, $r.p2 as timestamp, $r.p3 as host, $r.p4 as http_code, $r.p5 as message"
            + ")); "
            + "UPSERT INTO " + TABLE_NAME + " SELECT * FROM AS_TABLE(ListMap($values, $mapper))";

    private static final int BATCH_SIZE = 1000;
    private static final int BATCH_COUNT = 50;

    private static final Instant NOW = Instant.now();

    private static void dropTable(Connection connection) {
        LOG.info("Trying to drop table {}", TABLE_NAME);

        String dropSQL = String.format("DROP TABLE %s", TABLE_NAME);
        try (Statement statement = connection.createStatement()) {
            statement.execute("--jdbc:SCHEME\n" + dropSQL);
        } catch (YdbExecutionStatusException e) {
            LOG.info("Failed to drop table {} with code {}", TABLE_NAME, e.getStatusCode());
        } catch (SQLException e) {
            LOG.warn("Failed to drop table {}", TABLE_NAME, e);
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        LOG.info("Creating table table {}", TABLE_NAME);

        try (Statement statement = connection.createStatement()) {
            statement.execute("--jdbc:SCHEME\n" + CREATE_TABLE_SQL);
        }

        LOG.info("Table {} was successfully created.", TABLE_NAME);
    }

    private static long selectCount(Connection connection) throws SQLException {
        String selectSQL = "SELECT count(*) AS cnt FROM " + TABLE_NAME;

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(selectSQL)) {
                if (!rs.next()) {
                    LOG.warn("empty response");
                    return 0;
                }

                long rowsCount = rs.getLong("cnt");
                LOG.info("Table has {} rows", rowsCount);
                return rowsCount;
            }
        }
    }

    private static List<LogRecord> getLogBatch(int offset) {
        List<LogRecord> list = new ArrayList<>(BATCH_SIZE);
        for (int idx = 0; idx < BATCH_SIZE; idx += 1) {
            String app = "App_" + String.valueOf(idx / 256);
            Instant timestamp = NOW.plusSeconds(offset * BATCH_SIZE + idx);
            String host = "192.168.0." + offset % 256;
            int httpCode = idx % 113 == 0 ? 404 : 200;
            String message = idx % 3 == 0 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1";

            list.add(new LogRecord(app, timestamp, host, httpCode, message));
        }
        return list;
    }

    private static void batchUpsert(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(BATCH_UPSERT_SQL)) {
            for (int idx = 0; idx < BATCH_COUNT; idx += 1) {
                List<LogRecord> batch = getLogBatch(idx);
                LOG.info("write blog batch with offset {}", idx);
                writeLogBatch(ps, batch);
            }
        }
    }

    private static void writeLogBatch(PreparedStatement ps, List<LogRecord> batch) throws SQLException {
        for (LogRecord log : batch) {
            ps.setString(1, log.app());
            ps.setTimestamp(2, Timestamp.from(log.timestamp()));
            ps.setString(3, log.host());
            ps.setInt(4, log.hashCode());
            ps.setString(5, log.message());

            ps.addBatch();
        }

        executeWithRetry(() -> {
            ps.executeBatch();
            ps.clearBatch();
        });
    }

    private interface SQLRunnable {
        void run() throws SQLException;
    }

    private static void executeWithRetry(SQLRunnable execution) throws SQLException {
        final int maxRetries = 10;
        final int waitTimeCeilingMs = 5000;

        int retryCount = 0;
        int waitTimeMs = 100;

        while (retryCount <= maxRetries) {
            try {
                execution.run();
                return;
            } catch (YdbRetryableException re) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOG.error("Retries executing batch exceeded: " + re.getCause().getMessage());
                    throw re;
                }

                try {
                    Thread.sleep(waitTimeMs);
                } catch (InterruptedException e) {
                    throw re;
                }

                waitTimeMs = Math.min(waitTimeMs * 2, waitTimeCeilingMs);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-batch-upsert.jar <connection_url>");
            return;
        }

        String connectionUrl = args[0];

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            dropTable(connection);
            createTable(connection);
            batchUpsert(connection);

            long rowsCount = selectCount(connection);
            assert(rowsCount == BATCH_COUNT * BATCH_SIZE);
        } catch (SQLException e) {
            LOG.error("JDBC Example problem", e);
        }
    }
}
