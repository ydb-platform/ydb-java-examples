package tech.ydb.slo.core;

public final class Config {
    private final String connectionString;
    private final String jdbcUrl;
    private final String token;
    private final String ref;
    private final String workloadName;
    private final int durationSeconds;
    private final String otlpEndpoint;

    private Config(
            String connectionString,
            String jdbcUrl,
            String token,
            String ref,
            String workloadName,
            int durationSeconds,
            String otlpEndpoint
    ) {
        this.connectionString = connectionString;
        this.jdbcUrl = jdbcUrl;
        this.token = token;
        this.ref = ref;
        this.workloadName = workloadName;
        this.durationSeconds = durationSeconds;
        this.otlpEndpoint = otlpEndpoint;
    }

    public String connectionString() {
        return connectionString;
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String token() {
        return token;
    }

    public String ref() {
        return ref;
    }

    public String workloadName() {
        return workloadName;
    }

    public int durationSeconds() {
        return durationSeconds;
    }

    public String otlpEndpoint() {
        return otlpEndpoint;
    }

    public static Config fromEnv(String defaultWorkloadName) {
        String connectionString = resolveConnectionString();
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IllegalStateException(
                    "YDB connection is not configured: set YDB_CONNECTION_STRING, "
                            + "YDB_JDBC_URL or YDB_ENDPOINT + YDB_DATABASE"
            );
        }

        String token = envOrDefault("YDB_TOKEN", "");
        String ref = envOrDefault("WORKLOAD_REF", "unknown");
        String workloadName = envOrDefault("WORKLOAD_NAME", defaultWorkloadName);
        int durationSeconds = parseInt(envOrDefault("WORKLOAD_DURATION", "600"), 600);
        String otlpEndpoint = envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "");

        return new Config(
                connectionString,
                toJdbcUrl(connectionString),
                token,
                ref,
                workloadName,
                durationSeconds,
                otlpEndpoint
        );
    }

    private static String resolveConnectionString() {
        String jdbc = System.getenv("YDB_JDBC_URL");
        if (jdbc != null && !jdbc.isEmpty()) {
            return stripJdbcPrefix(jdbc);
        }

        String cs = System.getenv("YDB_CONNECTION_STRING");
        if (cs != null && !cs.isEmpty()) {
            return stripJdbcPrefix(cs);
        }

        String endpoint = System.getenv("YDB_ENDPOINT");
        String database = System.getenv("YDB_DATABASE");
        if (endpoint == null || endpoint.isEmpty() || database == null || database.isEmpty()) {
            return null;
        }
        return composeConnectionString(endpoint, database);
    }

    private static String stripJdbcPrefix(String value) {
        if (value.startsWith("jdbc:ydb:")) {
            return value.substring("jdbc:ydb:".length());
        }
        return value;
    }

    private static String toJdbcUrl(String connectionString) {
        if (connectionString.startsWith("jdbc:")) {
            return connectionString;
        }
        return "jdbc:ydb:" + connectionString;
    }

    private static String composeConnectionString(String endpoint, String database) {
        if (endpoint.endsWith("/") && database.startsWith("/")) {
            return endpoint + database.substring(1);
        }
        if (!endpoint.endsWith("/") && !database.startsWith("/")) {
            return endpoint + "/" + database;
        }
        return endpoint + database;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
