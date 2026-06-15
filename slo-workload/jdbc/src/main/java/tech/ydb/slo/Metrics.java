package tech.ydb.slo;

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

/**
 * Collects and pushes SLO workload metrics to the OTLP endpoint configured by
 * the YDB SLO action runtime.
 *
 * <p>Metrics emitted (matching the contract from
 * {@code ydb-platform/ydb-slo-action}):
 * <ul>
 *   <li>{@code sdk.operations.total} — counter, labeled by
 *       {@code operation_type} and {@code operation_status}</li>
 *   <li>{@code sdk.errors.total} — counter, labeled by
 *       {@code operation_type} and {@code error_kind}</li>
 *   <li>{@code sdk.retry.attempts.total} — counter, labeled by
 *       {@code operation_type} and {@code operation_status}</li>
 *   <li>{@code sdk.pending.operations} — up/down counter, labeled by
 *       {@code operation_type}</li>
 *   <li>{@code sdk.operation.latency.p50.seconds} /
 *       {@code .p95.seconds} / {@code .p99.seconds} —
 *       observable gauges fed from per-operation HDR histograms</li>
 * </ul>
 *
 * <p>Every metric carries the {@code ref} label so the report action can
 * separate current and baseline series.
 */
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

    // HDR histograms record latencies in microseconds with high precision up to 60 s.
    private static final long HDR_MIN_MICROS = 1L;
    private static final long HDR_MAX_MICROS = 60L * 1_000_000L;
    private static final int HDR_SIGNIFICANT_DIGITS = 3;

    private final SdkMeterProvider meterProvider;
    private final String ref;
    private final LongCounter operationsTotal;
    private final LongCounter errorsTotal;
    private final LongCounter retryAttemptsTotal;
    private final LongUpDownCounter pendingOperations;

    private final Map<OperationType, Histogram> histograms = new ConcurrentHashMap<>();

    private Metrics(
            SdkMeterProvider meterProvider,
            String ref,
            LongCounter operationsTotal,
            LongCounter errorsTotal,
            LongCounter retryAttemptsTotal,
            LongUpDownCounter pendingOperations
    ) {
        this.meterProvider = meterProvider;
        this.ref = ref;
        this.operationsTotal = operationsTotal;
        this.errorsTotal = errorsTotal;
        this.retryAttemptsTotal = retryAttemptsTotal;
        this.pendingOperations = pendingOperations;
    }

    /*
     * Builds a {@code Metrics} instance configured to push OTLP metrics every
     * second to the endpoint from {@code config.otlpEndpoint()}. If the
     * endpoint is empty, all metrics are still observable in-process but never
     * exported.
     */
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

        // Pre-create one histogram per operation_type so the first export
        // already produces gauge series. We only track successful operations:
        // failure latency is dominated by retry budgets / timeouts and would
        // skew the percentiles without telling us anything useful about SDK
        // performance. The SLO action's metrics.yaml filters by
        // operation_status="success" anyway.
        for (OperationType type : OperationType.values()) {
            histograms.put(type, newHistogram());
        }

        // Build the three percentile gauges as raw observers — their values
        // are produced by a single batch callback below, which reads
        // p50/p95/p99 from the same histogram snapshot and then resets the
        // histogram. Reading all three percentiles from one snapshot avoids
        // races where p99 could be observed against a freshly-reset histogram
        // populated by p50, and resetting after each export means the gauge
        // reflects only latencies recorded during the last export interval —
        // matching the JS SDK's behaviour and avoiding cold-start tail drag
        // on the JVM (without reset, JIT-warmup outliers stick to p99 for
        // the rest of the run).
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

        Metrics metrics = new Metrics(
                provider,
                ref,
                operationsTotal,
                errorsTotal,
                retryAttemptsTotal,
                pendingOperations
        );
        metrics.histograms.putAll(histograms);
        return metrics;
    }

    private static String metricsEndpoint(String otlpEndpoint) {
        // OTLP HTTP exporter expects the full /v1/metrics path. The SLO action
        // sets OTEL_EXPORTER_OTLP_ENDPOINT to the base URL (e.g.
        // http://ydb-prometheus:9090/api/v1/otlp), so we append the suffix
        // unless the user has already provided it.
        String trimmed = otlpEndpoint.endsWith("/")
                ? otlpEndpoint.substring(0, otlpEndpoint.length() - 1)
                : otlpEndpoint;
        if (trimmed.endsWith("/v1/metrics")) {
            return trimmed;
        }
        return trimmed + "/v1/metrics";
    }

    /*
     * Records a started operation and returns a span used to record the
     * outcome.
     */
    public Span startOperation(OperationType type) {
        pendingOperations.add(1, Attributes.of(
                ATTR_REF, ref,
                ATTR_OPERATION_TYPE, type.label()
        ));
        return new Span(this, type, System.nanoTime());
    }

    /**
     * Forces a final flush of pending metrics. Should be called before exit
     * to make sure the report action sees the last seconds of data.
     */
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

        // Latency is recorded only for successful operations. Failed
        // operations spend most of their time inside the retry budget /
        // timeout machinery, so their latency reflects the retry policy
        // rather than the SDK's performance. Mixing those samples into the
        // percentile gauges produces noisy spikes during chaos scenarios
        // and tells us nothing actionable.
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

    /**
     * Observes p50/p95/p99 for every populated histogram in one go and then
     * resets the histogram. Called from a single OTel batch callback so all
     * three percentiles are read from a consistent snapshot — without that,
     * a concurrent record could land between the p50 and p99 reads and
     * produce inconsistent values across gauges.
     */
    private static void observeAndResetPercentiles(
            Map<OperationType, Histogram> histograms,
            String ref,
            ObservableDoubleMeasurement p50Out,
            ObservableDoubleMeasurement p95Out,
            ObservableDoubleMeasurement p99Out
    ) {
        for (Map.Entry<OperationType, Histogram> entry : histograms.entrySet()) {
            OperationType type = entry.getKey();
            Histogram histogram = entry.getValue();

            long p50Micros;
            long p95Micros;
            long p99Micros;
            if (histogram.getTotalCount() == 0) {
                continue;
            }
            p50Micros = histogram.getValueAtPercentile(50.0);
            p95Micros = histogram.getValueAtPercentile(95.0);
            p99Micros = histogram.getValueAtPercentile(99.0);
            histogram.reset();

            // Percentile gauges are always tagged with operation_status="success"
            // because we only record successful samples (see recordOutcome).
            // The SLO action's metrics.yaml filters on this same label, so the
            // gauges line up with what the report expects.
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

    /**
     * One in-flight operation. Call exactly one of the {@code finish} methods.
     */
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
