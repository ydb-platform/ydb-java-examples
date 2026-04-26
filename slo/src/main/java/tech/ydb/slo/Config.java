package tech.ydb.slo;

/**
 * Configuration for the SLO workload, populated from environment variables
 * provided by the YDB SLO action runtime.
 *
 * <p>The action sets these variables on the workload container:
 * <ul>
 *   <li>{@code YDB_CONNECTION_STRING} or {@code YDB_ENDPOINT} + {@code YDB_DATABASE} — YDB connection</li>
 *   <li>{@code WORKLOAD_REF} — value used as the {@code ref} label on all metrics</li>
 *   <li>{@code WORKLOAD_NAME} — workload name (also used as part of the table path)</li>
 *   <li>{@code WORKLOAD_DURATION} — workload run duration in seconds (0 = unlimited)</li>
 *   <li>{@code OTEL_EXPORTER_OTLP_ENDPOINT} — OTLP endpoint for pushing metrics</li>
 * </ul>
 */
public final class Config {
    private final String connectionString;
    private final String ref;
    private final String workloadName;
    private final int durationSeconds;
    private final String otlpEndpoint;

    private Config(
            String connectionString,
            String ref,
            String workloadName,
            int durationSeconds,
            String otlpEndpoint
    ) {
        this.connectionString = connectionString;
        this.ref = ref;
        this.workloadName = workloadName;
        this.durationSeconds = durationSeconds;
        this.otlpEndpoint = otlpEndpoint;
    }

    public String connectionString() {
        return connectionString;
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
     * @throws IllegalStateException if required variables are missing or invalid
     */
    public static Config fromEnv() {
        String connectionString = resolveConnectionString();
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IllegalStateException(
                    "YDB connection is not configured: set YDB_CONNECTION_STRING or YDB_ENDPOINT + YDB_DATABASE"
            );
        }

        String ref = envOrDefault("WORKLOAD_REF", "unknown");
        String workloadName = envOrDefault("WORKLOAD_NAME", "java-slo-workload");
        int durationSeconds = parseInt(envOrDefault("WORKLOAD_DURATION", "600"), 600);
        String otlpEndpoint = envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "");

        return new Config(connectionString, ref, workloadName, durationSeconds, otlpEndpoint);
    }

    private static String resolveConnectionString() {
        String cs = System.getenv("YDB_CONNECTION_STRING");
        if (cs != null && !cs.isEmpty()) {
            return cs;
        }

        String endpoint = System.getenv("YDB_ENDPOINT");
        String database = System.getenv("YDB_DATABASE");
        if (endpoint == null || endpoint.isEmpty() || database == null || database.isEmpty()) {
            return null;
        }

        // Compose connection string in the form expected by GrpcTransport.forConnectionString:
        // grpc://host:port/database
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
