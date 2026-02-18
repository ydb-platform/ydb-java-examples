package tech.ydb.apps;

import java.util.function.Consumer;

import io.grpc.ManagedChannelBuilder;

/**
 * Adds both metrics and tracing interceptors to YDB gRPC channels.
 */
public class GrpcObservability implements Consumer<ManagedChannelBuilder<?>> {
    @Override
    public void accept(ManagedChannelBuilder<?> builder) {
        new GrpcMetrics().accept(builder);
        new GrpcTracing().accept(builder);
    }
}
