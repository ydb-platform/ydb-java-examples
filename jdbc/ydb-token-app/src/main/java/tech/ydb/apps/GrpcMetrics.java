package tech.ydb.apps;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class GrpcMetrics implements Consumer<ManagedChannelBuilder<?>>, ClientInterceptor {
    private static final Counter.Builder REQUEST = Counter.builder("grpc.request");
    private static final Counter.Builder RESPONSE = Counter.builder("grpc.response");
    private static final Timer.Builder LATENCY = Timer.builder("grpc.latency")
            .publishPercentiles(0.5, 0.9, 0.95, 0.99);

    private static MeterRegistry REGISTRY = null;

    public static void init(MeterRegistry registry) {
        REGISTRY = registry;
    }

    @Override
    public void accept(ManagedChannelBuilder<?> t) {
        t.intercept(this);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        if (REGISTRY != null) {
            return new ProxyClientCall<>(REGISTRY, next, method, callOptions);
        }
        return next.newCall(method, callOptions);
    }

    private static class ProxyClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
        private final MeterRegistry registry;
        private final String method;
        private final String authority;
        private final ClientCall<ReqT, RespT> delegate;

        private ProxyClientCall(MeterRegistry registry, Channel channel, MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions) {
            this.registry = registry;
            this.method = method.getBareMethodName();
            this.authority = channel.authority();
            this.delegate = channel.newCall(method, callOptions);
        }

        @Override
        public void request(int numMessages) {
            delegate.request(numMessages);
        }

        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
            delegate.cancel(message, cause);
        }

        @Override
        public void halfClose() {
            delegate.halfClose();
        }

        @Override
        public void setMessageCompression(boolean enabled) {
            delegate.setMessageCompression(enabled);
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public Attributes getAttributes() {
            return delegate.getAttributes();
        }

        @Override
        public void start(Listener<RespT> listener, Metadata headers) {
            REQUEST.tag("method", method).tag("authority", authority).register(registry).increment();
            delegate.start(new ProxyListener(listener), headers);
        }

        @Override
        public void sendMessage(ReqT message) {
            delegate.sendMessage(message);
        }

        private class ProxyListener extends Listener<RespT> {
            private final Listener<RespT> delegate;
            private final long startedAt;

            public ProxyListener(Listener<RespT> delegate) {
                this.delegate = delegate;
                this.startedAt = System.currentTimeMillis();
            }


            @Override
            public void onHeaders(Metadata headers) {
                delegate.onHeaders(headers);
            }

            @Override
            public void onMessage(RespT message) {
                delegate.onMessage(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                long ms = System.currentTimeMillis() - startedAt;
                RESPONSE.tag("method", method).tag("authority", authority).tag("status", status.getCode().toString())
                        .register(registry).increment();
                LATENCY.tag("method", method).tag("authority", authority)
                        .register(registry).record(ms, TimeUnit.MILLISECONDS);
                delegate.onClose(status, trailers);
            }

            @Override
            public void onReady() {
                delegate.onReady();
            }
        }
    }
}
