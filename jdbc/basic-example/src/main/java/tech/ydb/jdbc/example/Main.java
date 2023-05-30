package tech.ydb.jdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.exception.YdbNonRetryableException;

public class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-basic-example.jar <connection_url>");
            return;
        }

        String connectionUrl = args[0];

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            String tableName = "jdbc_table_sample";
            System.out.println(String.format("Trying to drop table %s...", tableName));
            try {
                connection.createStatement()
                        .execute("--jdbc:SCHEME\n" +
                                String.format("drop table %s", tableName));
                System.out.println(String.format("Table %s was successfully dropped.\n", tableName));
            } catch (SQLException e) {
                if (e instanceof YdbNonRetryableException
                        && ((YdbNonRetryableException) e).getStatusCode() == StatusCode.SCHEME_ERROR ) {
                    System.out.println(String.format("Failed to drop table %s because it's not yet created.\n", tableName));
                } else {
                    System.err.println(String.format("Failed to drop table %s", tableName));
                    e.printStackTrace();
                    return;
                }
            }

            System.out.println(String.format("Creating table %s...", tableName));
            connection.createStatement()
                    .execute("--jdbc:SCHEME\n" +
                            String.format("create table %s(id Int32, value Utf8, primary key (id))", tableName));
            System.out.println(String.format("Table %s was successfully created.\n", tableName));

            System.out.println("Upserting 2 rows into table...");
            YdbPreparedStatement ps = connection
                    .prepareStatement("" +
                            "declare $p1 as Int32;\n" +
                            "declare $p2 as Utf8;\n" +
                            String.format("upsert into %s (id, value) values ($p1, $p2)", tableName))
                    .unwrap(YdbPreparedStatement.class);
            ps.setInt(1, 1);
            ps.setString(2, "value-1");
            ps.executeUpdate();

            ps.setInt("p1", 2);
            ps.setString("p2", "value-2");
            ps.executeUpdate();

            connection.commit();
            System.out.println("Rows upserted.\n");

            PreparedStatement select = connection
                    .prepareStatement(String.format("select count(1) as cnt from %s", tableName));

            {
                System.out.println("Selecting table rows count...");
                ResultSet rs = select.executeQuery();
                rs.next();
                long rowsCount = rs.getLong("cnt");
                assert(rowsCount == 2);
                System.out.println(String.format("Table has %d rows.\n", rowsCount));
            }

            System.out.println("Upserting 2 more rows into table...");
            YdbPreparedStatement psBatch = connection
                    .prepareStatement("" +
                            "declare $values as List<Struct<id:Int32,value:Utf8>>;\n" +
                            "upsert into jdbc_table_sample select * from as_table($values)")
                    .unwrap(YdbPreparedStatement.class);
            psBatch.setInt("id", 3);
            psBatch.setString("value", "value-3");
            psBatch.addBatch();

            psBatch.setInt("id", 4);
            psBatch.setString("value", "value-4");
            psBatch.addBatch();

            psBatch.executeBatch();

            connection.commit();
            System.out.println("Rows upserted.\n");

            {
                System.out.println("Selecting table rows count...");
                ResultSet rs = select.executeQuery();
                rs.next();
                long rowsCount = rs.getLong("cnt");
                assert(rowsCount == 4);
                System.out.println(String.format("Table has %d rows.\n", rowsCount));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
