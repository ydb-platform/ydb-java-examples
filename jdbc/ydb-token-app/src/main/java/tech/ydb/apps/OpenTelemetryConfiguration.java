package tech.ydb.apps;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenTelemetryConfiguration {

    @Bean(destroyMethod = "close")
    public OpenTelemetry openTelemetry(Config config) {
        Resource resource = Resource.getDefault().merge(Resource.builder()
                .put(AttributeKey.stringKey("service.name"), config.getOtelServiceName())
                .build());

        SdkTracerProviderBuilder tracerProviderBuilder = SdkTracerProvider.builder()
                .setResource(resource);

        if (config.isOtelEnabled()) {
            SpanExporter exporter = OtlpGrpcSpanExporter.builder()
                    .setEndpoint(config.getOtelEndpoint())
                    .build();
            tracerProviderBuilder.addSpanProcessor(BatchSpanProcessor.builder(exporter).build());
        } else {
            tracerProviderBuilder.setSampler(Sampler.alwaysOff());
        }

        SdkTracerProvider tracerProvider = tracerProviderBuilder.build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }
}
