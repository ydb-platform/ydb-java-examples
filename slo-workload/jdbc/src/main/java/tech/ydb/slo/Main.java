package tech.ydb.slo;

import tech.ydb.slo.core.Launcher;
import tech.ydb.slo.jdbc.JdbcKvClient;

public final class Main {
    private static final String YDB_DRIVER_CLASS = "tech.ydb.jdbc.YdbDriver";

    private Main() {

    }

    public static void main(String[] args) {
        try {
            Class.forName(YDB_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            org.slf4j.LoggerFactory.getLogger(Main.class)
                    .error("YDB JDBC driver not found on classpath: {}", YDB_DRIVER_CLASS);
            System.exit(1);
            return;
        }

        Launcher.launch(
                "ydb-slo-jdbc-workload",
                "java-jdbc-kv",
                args,
                (config, params, tablePath) -> new JdbcKvClient(config, params, tablePath)
        );
    }
}
