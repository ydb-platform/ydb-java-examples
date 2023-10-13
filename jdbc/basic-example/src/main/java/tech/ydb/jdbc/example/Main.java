package tech.ydb.jdbc.example;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.LogManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // Enable redirect Java Util Logging to SLF4J
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("").setLevel(Level.FINEST);

        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-basic-example.jar <connection_url>");
            return;
        }

        String connectionUrl = args[0];

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            try {
                dropTable(connection);
            } catch (SQLException ex) {
                logger.warn("Can't drop table with message {}", ex.getMessage());
            }

            createTable(connection);

            simpleInsert(connection);
            select(connection);
            assertRowsCount(2, selectCount(connection));

            batchInsert(connection);
            select(connection);
            assertRowsCount(4, selectCount(connection));

            updateInTransaction(connection);
            select(connection);
            assertRowsCount(4, selectCount(connection));

            deleteEmpty(connection);
            select(connection);
            assertRowsCount(2, selectCount(connection));

        } catch (SQLException e) {
            logger.error("JDBC Example problem", e);
        }
    }

    private static void dropTable(Connection connection) throws SQLException {
        logger.info("Trying to drop table...");

        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE jdbc_basic_example");
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        logger.info("Creating table table jdbc_basic_example");

        try (Statement statement = connection.createStatement()) {
            statement.execute(""
                    + "CREATE TABLE jdbc_basic_example ("
                    + "  id Int32 NOT NULL, "
                    + "  c_text Text, "
                    + "  c_instant Timestamp, "
                    + "  c_date Date, "
                    + "  c_bytes Bytes, "
                    + "  PRIMARY KEY(id)"
                    + ")"
            );
        }

        logger.info("Table jdbc_basic_example was successfully created.");
    }

    private static long selectCount(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) AS cnt FROM jdbc_basic_example")) {
                if (!rs.next()) {
                    logger.warn("empty response");
                    return 0;
                }

                long rowsCount = rs.getLong("cnt");
                logger.info("Table has {} rows", rowsCount);
                return rowsCount;
            }
        }
    }

    private static void select(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM jdbc_basic_example")) {
                while (rs.next()) {
                    logger.info("read new row with id {}", rs.getInt("id"));
                    logger.info("   text    = {}", rs.getString("c_text"));
                    logger.info("   instant = {}", rs.getTimestamp("c_instant"));
                    logger.info("   date    = {}", String.valueOf(rs.getDate("c_date")));
                    logger.info("   bytes   = {}", rs.getBytes("c_bytes"));
                }
            }
        }
    }


    private static void simpleInsert(Connection connection) throws SQLException {
        logger.info("Inserting 2 rows into table...");

        Instant instant = Instant.parse("2023-04-03T12:30:25.000Z");
        byte[] byteArray = { (byte)0x00, (byte)0x23, (byte)0x45, (byte)0x98 };

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO jdbc_basic_example (id, c_text, c_instant, c_date, c_bytes) VALUES (?, ?, ?, ?, ?)"
        )) {
            // Insert row with data
            ps.setInt(1, 1);
            ps.setString(2, "Text one");
            ps.setTimestamp(3, Timestamp.from(instant));
            ps.setDate(4, new Date(instant.toEpochMilli()));
            ps.setBytes(5, byteArray);
            ps.executeUpdate();

            // Insert row without data - all columns except id are NULL
            ps.setInt(1, 2);
            ps.setString(2, null);
            ps.setTimestamp(3, null);
            ps.setDate(4, null);
            ps.setBytes(5, null);
            ps.executeUpdate();
        }

        logger.info("Rows inserted.");
    }

    private static void batchInsert(Connection connection) throws SQLException {
        logger.info("Inserting 2 more rows into table...");

        Instant instant = Instant.parse("2002-02-20T13:44:55.123Z");
        byte[] byteArray = { (byte)0x32, (byte)0x00, (byte)0x89, (byte)0x54 };

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO jdbc_basic_example (id, c_text, c_instant, c_date, c_bytes) VALUES (?, ?, ?, ?, ?)"
        )) {
            // Add row with data to batch
            ps.setInt(1, 3);
            ps.setString(2, "Other text");
            ps.setTimestamp(3, Timestamp.from(instant));
            ps.setDate(4, new Date(instant.toEpochMilli()));
            ps.setBytes(5, byteArray);
            ps.addBatch();

            // Add row without data to batch
            ps.setInt(1, 4);
            ps.setString(2, null);
            ps.setTimestamp(3, null);
            ps.setDate(4, null);
            ps.setBytes(5, null);
            ps.addBatch();

            // Execute batch
            ps.executeBatch();
        }

        logger.info("Rows inserted.");
    }

    private static void updateInTransaction(Connection connection) throws SQLException {
        logger.info("Update some rows in transaction...");

        connection.setAutoCommit(false);

        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE jdbc_basic_example SET c_text = ? WHERE id = ?"
        )) {
            ps.setString(1, "Updated text");
            ps.setInt(2, 1);
            ps.executeUpdate();

            ps.setString(1, "New text");
            ps.setInt(2, 2);
            ps.executeUpdate();

            connection.commit();

            ps.setString(1, "Old text");
            ps.setInt(2, 1);
            ps.executeUpdate();

            connection.rollback();
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private static void deleteEmpty(Connection connection) throws SQLException {
        logger.info("Deleting empty rows from into table...");

        try (Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM jdbc_basic_example WHERE c_instant IS NULL");
        }
    }

    private static void assertRowsCount(long rowsCount, long expectedRows) {
        if (rowsCount != expectedRows) {
            throw new AssertionError("Unexpected count of rows, expected " + expectedRows + ", but got " + rowsCount);
        }
    }
}
