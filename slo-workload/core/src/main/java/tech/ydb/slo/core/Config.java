package tech.ydb.slo.core;

/**
 * Configuration for the SLO workloads, populated from environment variables
 * provided by the YDB SLO action runtime.
 *
 * <p>The action sets these variables on the workload container:
 * <ul>
 *   <li>{@code YDB_CONNECTION_STRING} or {@code YDB_ENDPOINT} + {@code YDB_DATABASE} — YDB connection</li>
 *   <li>{@code YDB_TOKEN} — optional access token</li>
 *   <li>{@code WORKLOAD_REF} — value used as the {@code ref} label on all metrics</li>
 *   <li>{@code WORKLOAD_NAME} — workload name (also used as part of the table path)</li>
 *   <li>{@code WORKLOAD_DURATION} — workload run duration in seconds (0 = unlimited)</li>
 *   <li>{@code OTEL_EXPORTER_OTLP_ENDPOINT} — OTLP endpoint for pushing metrics</li>
 * </ul>
 *
 * <p>The same configuration object is shared by every workload, regardless of
 * the client it exercises. It therefore exposes the connection both as a YDB
 * connection string ({@code grpc://host:port/database}, consumed by the native
 * SDK transport) and as a JDBC URL ({@code jdbc:ydb:...}, consumed by the JDBC
 * driver and the Spring Data workloads).
 */
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

    /**
     * YDB connection string in the {@code grpc://host:port/database} form,
     * consumed by the native SDK transport.
     * @return connection string, never empty
     */
    public String connectionString() {
        return connectionString;
    }

    /**
     * YDB JDBC URL ({@code jdbc:ydb:...}), consumed by the JDBC driver and the
     * Spring Data workloads.
     * @return JDBC URL, never empty
     */
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

    /**
     * Loads configuration from environment variables.
     *
     * @param defaultWorkloadName workload name to use when {@code WORKLOAD_NAME} is not set
     * @return configuration instance
     * @throws IllegalStateException if the YDB connection is not configured
     */
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

    /*
     * Resolves the connection in grpc://host:port/database form. YDB_JDBC_URL,
     * if provided, is normalized back to a plain connection string so the
     * native transport can use it too.
     */
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

    /**
     * Turns a YDB connection string ({@code grpc://host:port/database}) into a
     * JDBC URL understood by the YDB JDBC driver. If the value already starts
     * with {@code jdbc:}, it is returned unchanged.
     */
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
