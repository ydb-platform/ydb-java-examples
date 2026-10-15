package tech.ydb.slo.core;

/** In-process metrics only: no receiver, transport or environment dependency. */
public final class TestMetrics {
    private TestMetrics() { }

    public static Metrics create() {
        return Metrics.create(new Config("grpc://localhost:1/local", "", "", "unit", "query-rmw-test", 0, ""));
    }
}
