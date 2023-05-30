package tech.ydb.jdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import tech.ydb.jdbc.exception.YdbExecutionStatusException;

public class Main {
    private final static Logger LOG = LoggerFactory.getLogger(Main.class);
    private final static String TABLE_NAME = "jdbc_table_sample";

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

        String createSQL = "CREATE TABLE " + TABLE_NAME + "(id Int32, value Text, PRIMARY KEY(id))";

        try (Statement statement = connection.createStatement()) {
            statement.execute("--jdbc:SCHEME\n" + createSQL);
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

    private static void simpleUpsert(Connection connection) throws SQLException {
        LOG.debug("Upserting 2 rows into table...");

        String upsertSQL = "UPSERT INTO " + TABLE_NAME + " (id, value) values (?, ?)";

        try (PreparedStatement ps = connection.prepareStatement(upsertSQL)) {
            ps.setInt(1, 1);
            ps.setString(2, "value-1");
            ps.executeUpdate();

            ps.setInt(1, 2);
            ps.setString(2, "value-2");
            ps.executeUpdate();
        }

        LOG.debug("Rows upserted.");
    }

    private static void batchUpsert(Connection connection) throws SQLException {
        LOG.debug("Upserting 2 more rows into table...");

        String batchUpsertSQL = ""
                + "DECLARE $values AS List<Struct<p1:Int32, p2:Text>>;\n"
                + "$mapper = ($row) -> (AsStruct($row.p1 as id, $row.p2 as value));\n"
                + "UPSERT INTO " + TABLE_NAME + " SELECT * FROM AS_TABLE(ListMap($values, $mapper))";

        try (PreparedStatement ps = connection.prepareStatement(batchUpsertSQL)) {
            ps.setInt(1, 3);
            ps.setString(2, "value-3");
            ps.addBatch();

            ps.setInt(1, 4);
            ps.setString(2, "value-4");
            ps.addBatch();

            ps.executeBatch();
        }

        LOG.debug("Rows upserted.");
    }

    public static void main(String[] args) {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("").setLevel(Level.FINEST);

        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-basic-example.jar <connection_url>");
            return;
        }

        String connectionUrl = args[0];

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            dropTable(connection);
            createTable(connection);

            simpleUpsert(connection);

            long rowsCount = selectCount(connection);
            assert(rowsCount == 2);

            batchUpsert(connection);

            rowsCount = selectCount(connection);
            assert(rowsCount == 4);
        } catch (SQLException e) {
            LOG.error("JDBC Example problem", e);
        }
    }
}
