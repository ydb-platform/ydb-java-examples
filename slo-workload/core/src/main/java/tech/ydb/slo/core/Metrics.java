package tech.ydb.slo.core;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

public final class Metrics implements AutoCloseable {

    public enum OperationType {
        READ("read"),
        WRITE("write");

        private final String label;

        OperationType(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    public enum OperationStatus {
        SUCCESS("success"),
        ERROR("error");

        private final String label;

        OperationStatus(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private static final AttributeKey<String> ATTR_OPERATION_TYPE =
            AttributeKey.stringKey("operation_type");
    private static final AttributeKey<String> ATTR_OPERATION_STATUS =
            AttributeKey.stringKey("operation_status");
    private static final AttributeKey<String> ATTR_ERROR_KIND =
            AttributeKey.stringKey("error_kind");
    private static final AttributeKey<String> ATTR_REF =
            AttributeKey.stringKey("ref");


    private static final long HDR_MIN_MICROS = 1L;
    private static final long HDR_MAX_MICROS = 60L * 1_000_000L;
    private static final int HDR_SIGNIFICANT_DIGITS = 3;

    private final SdkMeterProvider meterProvider;
    private final String ref;
    private final LongCounter operationsTotal;
    private final LongCounter errorsTotal;
    private final LongCounter retryAttemptsTotal;
    private final LongUpDownCounter pendingOperations;

    private final Map<OperationType, Histogram> histograms;

    private Metrics(
            SdkMeterProvider meterProvider,
            String ref,
            LongCounter operationsTotal,
            LongCounter errorsTotal,
            LongCounter retryAttemptsTotal,
            LongUpDownCounter pendingOperations,
            Map<OperationType, Histogram> histograms
    ) {
        this.meterProvider = meterProvider;
        this.ref = ref;
        this.operationsTotal = operationsTotal;
        this.errorsTotal = errorsTotal;
        this.retryAttemptsTotal = retryAttemptsTotal;
        this.pendingOperations = pendingOperations;
        this.histograms = histograms;
    }



    public static Metrics create(Config config) {
        String ref = config.ref();

        Resource resource = Resource.getDefault().toBuilder()
                .put("service.name", config.workloadName())
                .put("ref", ref)
                .put("sdk", "java")
                .build();

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder()
                .setResource(resource);

        if (config.otlpEndpoint() != null && !config.otlpEndpoint().isEmpty()) {
            OtlpHttpMetricExporter exporter = OtlpHttpMetricExporter.builder()
                    .setEndpoint(metricsEndpoint(config.otlpEndpoint()))
                    .setTimeout(Duration.ofSeconds(10))
                    .build();
            providerBuilder.registerMetricReader(
                    PeriodicMetricReader.builder(exporter)
                            .setInterval(Duration.ofSeconds(1))
                            .build()
            );
        }

        SdkMeterProvider provider = providerBuilder.build();
        Meter meter = provider.get("slo-workload-" + config.workloadName());

        LongCounter operationsTotal = meter.counterBuilder("sdk.operations.total")
                .setDescription("Total number of operations")
                .setUnit("{operation}")
                .build();

        LongCounter errorsTotal = meter.counterBuilder("sdk.errors.total")
                .setDescription("Total number of errors")
                .setUnit("{error}")
                .build();

        LongCounter retryAttemptsTotal = meter.counterBuilder("sdk.retry.attempts.total")
                .setDescription("Total number of retry attempts")
                .setUnit("{attempt}")
                .build();

        LongUpDownCounter pendingOperations = meter.upDownCounterBuilder("sdk.pending.operations")
                .setDescription("Currently in-flight operations")
                .build();

        Map<OperationType, Histogram> histograms = new ConcurrentHashMap<>();







        for (OperationType type : OperationType.values()) {
            histograms.put(type, newHistogram());
        }











        ObservableDoubleMeasurement p50Observer = meter.gaugeBuilder("sdk.operation.latency.p50.seconds")
                .setUnit("s")
                .setDescription("p50 operation latency in seconds")
                .buildObserver();

        ObservableDoubleMeasurement p95Observer = meter.gaugeBuilder("sdk.operation.latency.p95.seconds")
                .setUnit("s")
                .setDescription("p95 operation latency in seconds")
                .buildObserver();

        ObservableDoubleMeasurement p99Observer = meter.gaugeBuilder("sdk.operation.latency.p99.seconds")
                .setUnit("s")
                .setDescription("p99 operation latency in seconds")
                .buildObserver();

        meter.batchCallback(
                () -> observeAndResetPercentiles(histograms, ref, p50Observer, p95Observer, p99Observer),
                p50Observer, p95Observer, p99Observer
        );

        return new Metrics(
                provider,
                ref,
                operationsTotal,
                errorsTotal,
                retryAttemptsTotal,
                pendingOperations,
                histograms
        );
    }

    private static String metricsEndpoint(String otlpEndpoint) {




        String trimmed = otlpEndpoint.endsWith("/")
                ? otlpEndpoint.substring(0, otlpEndpoint.length() - 1)
                : otlpEndpoint;
        if (trimmed.endsWith("/v1/metrics")) {
            return trimmed;
        }
        return trimmed + "/v1/metrics";
    }



    public Span startOperation(OperationType type) {
        pendingOperations.add(1, Attributes.of(
                ATTR_REF, ref,
                ATTR_OPERATION_TYPE, type.label()
        ));
        return new Span(this, type, System.nanoTime());
    }



    public void flush() {
        meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        meterProvider.shutdown().join(10, TimeUnit.SECONDS);
    }

    private void recordOutcome(
            OperationType type,
            OperationStatus status,
            int attempts,
            long latencyMicros,
            String errorKind
    ) {
        Attributes opAttrs = Attributes.of(
                ATTR_REF, ref,
                ATTR_OPERATION_TYPE, type.label(),
                ATTR_OPERATION_STATUS, status.label()
        );

        operationsTotal.add(1, opAttrs);
        retryAttemptsTotal.add(Math.max(0L, attempts), opAttrs);
        pendingOperations.add(-1, Attributes.of(
                ATTR_REF, ref,
                ATTR_OPERATION_TYPE, type.label()
        ));







        if (status == OperationStatus.SUCCESS) {
            Histogram histogram = histograms.computeIfAbsent(type, k -> newHistogram());
            long clamped = Math.max(HDR_MIN_MICROS, Math.min(HDR_MAX_MICROS, latencyMicros));
            histogram.recordValue(clamped);
        } else {
            errorsTotal.add(1, Attributes.of(
                    ATTR_REF, ref,
                    ATTR_OPERATION_TYPE, type.label(),
                    ATTR_ERROR_KIND, errorKind == null ? "unknown" : errorKind
            ));
        }
    }



    private static void observeAndResetPercentiles(
            Map<OperationType, Histogram> histograms,
            String ref,
            ObservableDoubleMeasurement p50Out,
            ObservableDoubleMeasurement p95Out,
            ObservableDoubleMeasurement p99Out
    ) {
        for (Map.Entry<OperationType, Histogram> entry : histograms.entrySet()) {
            OperationType type = entry.getKey();
            Histogram live = entry.getValue();

            Histogram snapshot = live.copy();
            live.reset();
            if (snapshot.getTotalCount() == 0) {
                continue;
            }
            long p50Micros = snapshot.getValueAtPercentile(50.0);
            long p95Micros = snapshot.getValueAtPercentile(95.0);
            long p99Micros = snapshot.getValueAtPercentile(99.0);





            Attributes attrs = Attributes.of(
                    ATTR_REF, ref,
                    ATTR_OPERATION_TYPE, type.label(),
                    ATTR_OPERATION_STATUS, OperationStatus.SUCCESS.label()
            );
            p50Out.record(p50Micros / 1_000_000.0, attrs);
            p95Out.record(p95Micros / 1_000_000.0, attrs);
            p99Out.record(p99Micros / 1_000_000.0, attrs);
        }
    }

    private static Histogram newHistogram() {
        return new AtomicHistogram(HDR_MIN_MICROS, HDR_MAX_MICROS, HDR_SIGNIFICANT_DIGITS);
    }

    public static final class Span {
        private final Metrics metrics;
        private final OperationType type;
        private final long startNanos;
        private boolean finished;

        private Span(Metrics metrics, OperationType type, long startNanos) {
            this.metrics = metrics;
            this.type = type;
            this.startNanos = startNanos;
        }

        public void finishSuccess(int attempts) {
            finish(OperationStatus.SUCCESS, attempts, null);
        }

        public void finishError(int attempts, String errorKind) {
            finish(OperationStatus.ERROR, attempts, errorKind);
        }

        private void finish(OperationStatus status, int attempts, String errorKind) {
            if (finished) {
                return;
            }
            finished = true;
            long latencyMicros = (System.nanoTime() - startNanos) / 1_000L;
            metrics.recordOutcome(type, status, attempts, latencyMicros, errorKind);
        }
    }

}
