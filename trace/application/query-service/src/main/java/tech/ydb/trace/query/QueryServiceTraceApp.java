package tech.ydb.trace.query;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.tracing.OpenTelemetryTracer;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class QueryServiceTraceApp {
    private static final Logger logger = LoggerFactory.getLogger(QueryServiceTraceApp.class);
    private static final String TABLE = "bank_query_service";
    private static final String ACTIVITY_SOURCE = "Ydb.Sdk.Java.QueryService.OpenTelemetry.Sample";

    private QueryServiceTraceApp() {
    }

    public static void main(String[] args) {
        String connectionString = "grpc://ydb:2136/local";
        String serviceName = "ydb-sdk-sample-query";
        String otelEndpoint = "http://otel-collector:4317";
        String serviceVersion = resolveServiceVersion();

        OpenTelemetrySdk openTelemetry = createOpenTelemetry(serviceName, serviceVersion, otelEndpoint);
        Tracer appTracer = openTelemetry.getTracer(ACTIVITY_SOURCE);

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .withTracer(OpenTelemetryTracer.fromOpenTelemetry(openTelemetry))
                .build();
             QueryClient queryClient = QueryClient.newClient(transport).build()) {

            SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
            runScenario(retryCtx, appTracer, serviceName);
            System.out.println("App finished.");
        } finally {
            openTelemetry.close();
        }
    }

    private static void runScenario(SessionRetryContext retryCtx, Tracer appTracer, String serviceName) {
        System.out.println("[" + java.time.OffsetDateTime.now() + "] started, service.name=" + serviceName);
        System.out.println("Initializing...");

        Span startupSpan = appTracer.spanBuilder("app.startup").startSpan();
        try (Scope ignored = startupSpan.makeCurrent()) {
            startupSpan.setAttribute("app.message", "hello");
            safeDropTable(retryCtx);
            createTable(retryCtx);
        } finally {
            startupSpan.end();
        }

        System.out.println("Insert row...");
        retryCtx.supplyResult(session -> session.createQuery(
                "INSERT INTO " + TABLE + "(id, amount) VALUES (1, 0);",
                TxMode.SERIALIZABLE_RW
        ).execute()).join().getStatus().expectSuccess("insert failed");

        System.out.println("Preparing queries...");
        incrementInTransaction(retryCtx);

        System.out.println("Emulation TLI...");
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int taskNum = i;
            tasks.add(CompletableFuture.runAsync(() -> {
                Span concurrent = appTracer.spanBuilder("example_tli").startSpan();
                try (Scope ignored = concurrent.makeCurrent()) {
                    concurrent.setAttribute("app.message", "concurrent task " + taskNum);
                    incrementInTransaction(retryCtx);
                } finally {
                    concurrent.end();
                }
            }));
        }
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();

        System.out.println("Retry connection example...");
        int amount = readAmount(retryCtx);
        logger.info("Current amount={}", amount);
    }

    private static void createTable(SessionRetryContext retryCtx) {
        retryCtx.supplyResult(session -> session.createQuery(
                "CREATE TABLE " + TABLE + "(id Int32, amount Int32, PRIMARY KEY (id));",
                TxMode.NONE
        ).execute()).join().getStatus().expectSuccess("create table failed");
    }

    private static void safeDropTable(SessionRetryContext retryCtx) {
        try {
            retryCtx.supplyResult(session -> session.createQuery(
                    "DROP TABLE " + TABLE + ";",
                    TxMode.NONE
            ).execute()).join().getStatus().expectSuccess("drop table failed");
        } catch (RuntimeException ex) {
            logger.debug("Drop table skipped: {}", ex.getMessage());
        }
    }

    private static void incrementInTransaction(SessionRetryContext retryCtx) {
        retryCtx.supplyResult(session -> {
            QueryTransaction tx = session.beginTransaction(TxMode.SERIALIZABLE_RW).join().getValue();
            int count = readAmount(tx);
            tx.createQuery(
                    "DECLARE $amount AS Int32; UPDATE " + TABLE + " SET amount = $amount + 1 WHERE id = 1;",
                    Params.of("$amount", PrimitiveValue.newInt32(count))
            ).execute().join().getStatus().expectSuccess("update failed");
            return tx.commit();
        }).join().getStatus().expectSuccess("transaction failed");
    }

    private static int readAmount(SessionRetryContext retryCtx) {
        QueryReader reader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(
                        "SELECT amount FROM " + TABLE + " WHERE id = 1;",
                        TxMode.SNAPSHOT_RO
                )
        )).join().getValue();
        ResultSetReader rs = reader.getResultSet(0);
        if (!rs.next()) {
            throw new IllegalStateException("row not found");
        }
        return rs.getColumn("amount").getInt32();
    }

    private static int readAmount(QueryTransaction tx) {
        QueryReader reader = QueryReader.readFrom(
                tx.createQuery("SELECT amount FROM " + TABLE + " WHERE id = 1;")
        ).join().getValue();
        ResultSetReader rs = reader.getResultSet(0);
        if (!rs.next()) {
            throw new IllegalStateException("row not found");
        }
        return rs.getColumn("amount").getInt32();
    }

    private static OpenTelemetrySdk createOpenTelemetry(String serviceName, String serviceVersion, String otelEndpoint) {
        Resource resource = Resource.getDefault().merge(Resource.builder()
                .put(AttributeKey.stringKey("service.name"), serviceName)
                .put(AttributeKey.stringKey("service.version"), serviceVersion)
                .build());

        SpanExporter exporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(otelEndpoint)
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    private static String resolveServiceVersion() {
        Package pkg = QueryServiceTraceApp.class.getPackage();
        String version = pkg == null ? null : pkg.getImplementationVersion();
        return version == null || version.isEmpty() ? "unknown" : version;
    }
}
