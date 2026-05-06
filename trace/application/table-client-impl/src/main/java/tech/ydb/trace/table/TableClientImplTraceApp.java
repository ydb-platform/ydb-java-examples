package tech.ydb.trace.table;

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
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.tracing.OpenTelemetryTracer;
import tech.ydb.query.QueryClient;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class TableClientImplTraceApp {
    private static final Logger logger = LoggerFactory.getLogger(TableClientImplTraceApp.class);
    private static final String TABLE = "bank_table_service";
    private static final String ACTIVITY_SOURCE = "Ydb.Sdk.Java.TableClientImpl.OpenTelemetry.Sample";

    private TableClientImplTraceApp() { }

    public static void main(String[] args) {
        String connectionString = "grpc://ydb:2136/local";
        String serviceName = "ydb-sdk-sample-table";
        String otelEndpoint = "http://otel-collector:4317";
        String serviceVersion = resolveServiceVersion();

        OpenTelemetrySdk openTelemetry = createOpenTelemetry(serviceName, serviceVersion, otelEndpoint);
        Tracer appTracer = openTelemetry.getTracer(ACTIVITY_SOURCE);

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .withTracer(OpenTelemetryTracer.fromOpenTelemetry(openTelemetry))
                .build();
             TableClient tableClient = QueryClient.newTableClient(transport).build()) {

            SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
            runScenario(transport.getDatabase(), retryCtx, appTracer, serviceName);
            System.out.println("App finished.");
        } finally {
            openTelemetry.close();
        }
    }

    private static void runScenario(String database, SessionRetryContext retryCtx, Tracer appTracer, String serviceName) {
        String tablePath = database + "/" + TABLE;
        System.out.println("[" + java.time.OffsetDateTime.now() + "] started, service.name=" + serviceName);
        System.out.println("Initializing...");

        Span startupSpan = appTracer.spanBuilder("app.startup").startSpan();
        try (Scope ignored = startupSpan.makeCurrent()) {
            startupSpan.setAttribute("app.message", "hello");
            safeDropTable(tablePath, retryCtx);
            createTable(tablePath, retryCtx);
        } finally {
            startupSpan.end();
        }

        System.out.println("Insert row...");
        insertRow(retryCtx);

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

    private static void createTable(String tablePath, SessionRetryContext retryCtx) {
        TableDescription tableDescription = TableDescription.newBuilder()
                .addNonnullColumn("id", PrimitiveType.Int32)
                .addNullableColumn("amount", PrimitiveType.Int32)
                .setPrimaryKey("id")
                .build();
        retryCtx.supplyStatus(session -> session.createTable(tablePath, tableDescription))
                .join().expectSuccess("create table failed");
    }

    private static void safeDropTable(String tablePath, SessionRetryContext retryCtx) {
        try {
            retryCtx.supplyStatus(session -> session.dropTable(tablePath))
                    .join().expectSuccess("drop table failed");
        } catch (RuntimeException ex) {
            logger.debug("Drop table skipped: {}", ex.getMessage());
        }
    }

    private static void insertRow(SessionRetryContext retryCtx) {
        TxControl<?> txControl = TxControl.serializableRw().setCommitTx(true);
        retryCtx.supplyResult(session -> session.executeDataQuery(
                "DECLARE $id AS Int32; DECLARE $amount AS Int32; "
                        + "INSERT INTO " + TABLE + "(id, amount) VALUES ($id, $amount);",
                txControl,
                Params.of("$id", PrimitiveValue.newInt32(1), "$amount", PrimitiveValue.newInt32(0))
        )).join().getStatus().expectSuccess("insert failed");
    }

    private static void incrementInTransaction(SessionRetryContext retryCtx) {
        retryCtx.supplyStatus(session -> {
            TableTransaction tx = session.createNewTransaction(tech.ydb.common.transaction.TxMode.SERIALIZABLE_RW);
            int count = readAmount(tx);
            tx.executeDataQuery(
                    "DECLARE $amount AS Int32; UPDATE " + TABLE + " SET amount = $amount + 1 WHERE id = 1",
                    Params.of("$amount", PrimitiveValue.newInt32(count))
            ).join().getStatus().expectSuccess("update failed");
            return tx.commit();
        }).join().expectSuccess("transaction failed");
    }

    private static int readAmount(SessionRetryContext retryCtx) {
        DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(
                "SELECT amount FROM " + TABLE + " WHERE id = 1",
                TxControl.snapshotRo().setCommitTx(true)
        )).join().getValue();

        ResultSetReader rs = result.getResultSet(0);
        if (!rs.next()) {
            throw new IllegalStateException("row not found");
        }
        return rs.getColumn("amount").getInt32();
    }

    private static int readAmount(TableTransaction tx) {
        DataQueryResult result = tx.executeDataQuery(
                "SELECT amount FROM " + TABLE + " WHERE id = 1"
        ).join().getValue();
        ResultSetReader rs = result.getResultSet(0);
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
        Package pkg = TableClientImplTraceApp.class.getPackage();
        String version = pkg == null ? null : pkg.getImplementationVersion();
        return version == null || version.isEmpty() ? "unknown" : version;
    }
}
