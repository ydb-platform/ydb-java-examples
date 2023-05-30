package tech.ydb.jdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.jul.Log4jBridgeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.jdbc.exception.YdbExecutionStatusException;

public class Main {
    private final static Logger LOG = LoggerFactory.getLogger(Main.class);
    private final static String TABLE_NAME = "jdbc_batch_example";

    private final static String CREATE_TABLE_SQL = ""
            + "CREATE TABLE " + TABLE_NAME + "("
            + "app Text,"
            + "timestamp Timestamp,"
            + "host Text,"
            + "http_code UInt32,"
            + "message Text,"
            + "PRIMARY KEY(app, timestamp, host))";

    private static void dropTable(Connection connection) {
        LOG.debug("Trying to drop table {}", TABLE_NAME);

        String dropSQL = String.format("DROP TABLE %s", TABLE_NAME);
        try (Statement statement = connection.createStatement()) {
            statement.execute("--jdbc:SCHEME\n" + dropSQL);
        } catch (YdbExecutionStatusException e) {
            LOG.debug("Failed to drop table {} with code {}", TABLE_NAME, e.getStatusCode());
        } catch (SQLException e) {
            LOG.warn("Failed to drop table {}", TABLE_NAME, e);
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        LOG.debug("Creating table table {}", TABLE_NAME);

        try (Statement statement = connection.createStatement()) {
            statement.execute("--jdbc:SCHEME\n" + CREATE_TABLE_SQL);
        }

        LOG.debug("Table {} was successfully created.", TABLE_NAME);
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
                LOG.debug("Table has {} rows", rowsCount);
                return rowsCount;
            }
        }
    }

    public static void main(String[] args) {
        Log4jBridgeHandler.install(true, "tech.ydb", false);

        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-batch-upsert.jar <connection_url>");
            return;
        }

        String connectionUrl = args[0];

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            dropTable(connection);
            createTable(connection);

//            simpleUpsert(connection);
//
//            long rowsCount = selectCount(connection);
//            assert(rowsCount == 2);
//
//            batchUpsert(connection);

            long rowsCount = selectCount(connection);
            assert(rowsCount == 0);
        } catch (SQLException e) {
            LOG.error("JDBC Example problem", e);
        }
    }
}
