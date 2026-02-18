package tech.ydb.apps;

import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;

import java.util.function.Consumer;

/**
 * gRPC client tracing based on official OpenTelemetry gRPC interceptor.
 */
public class GrpcTracing implements Consumer<ManagedChannelBuilder<?>> {
    private static volatile OpenTelemetry openTelemetry = OpenTelemetry.noop();

    public static void init(OpenTelemetry telemetry) {
        openTelemetry = telemetry;
    }

    @Override
    public void accept(ManagedChannelBuilder<?> builder) {
        builder.intercept(GrpcTelemetry.create(openTelemetry).newClientInterceptor());
    }
}
