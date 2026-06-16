package tech.ydb.slo.query;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.slo.core.Config;
import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvSchema;
import tech.ydb.slo.core.kv.KvSession;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.OpOutcome;
import tech.ydb.slo.core.kv.Row;
import tech.ydb.slo.core.kv.RowGenerator;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

/**
 * {@link KvClient} backed by the native YDB query client and
 * {@link SessionRetryContext}.
 *
 * <p>The {@code hash} primary-key column is computed client-side via
 * {@link RowGenerator#numericHash(long)} (rather than YQL's
 * {@code Digest::NumericHash}) so a table written by this workload is
 * byte-compatible with the JDBC and Spring Data workloads. The exact mixing
 * function doesn't matter to the workload — only that every implementation
 * agrees.
 */
public final class QueryKvClient implements KvClient {
    private static final String WRITE_QUERY_TEMPLATE = ""
            + "DECLARE $hash AS Uint64;"
            + "DECLARE $id AS Uint64;"
            + "DECLARE $payload_str AS Utf8;"
            + "DECLARE $payload_double AS Double;"
            + "DECLARE $payload_timestamp AS Timestamp;"
            + "DECLARE $payload_hash AS Uint64;"
            + "UPSERT INTO `%s` ("
            + "  hash, id, payload_str, payload_double, payload_timestamp, payload_hash"
            + ") VALUES ("
            + "  $hash,"
            + "  $id,"
            + "  $payload_str,"
            + "  $payload_double,"
            + "  $payload_timestamp,"
            + "  $payload_hash"
            + ");";

    private static final String READ_QUERY_TEMPLATE = ""
            + "DECLARE $id AS Uint64;"
            + "DECLARE $hash AS Uint64;"
            + "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
            + "  FROM `%s`"
            + "  WHERE id = $id AND hash = $hash;";

    private final SessionRetryContext retryCtx;
    private final String tablePath;
    private final GrpcTransport transport;
    private final QueryClient queryClient;

    public QueryKvClient(Config config, KvWorkloadParams params, String tablePath) {
        this.tablePath = tablePath;
        AuthProvider provider = NopAuthProvider.INSTANCE;
        if (config.token() != null && !config.token().isEmpty()) {
            provider = new TokenAuthProvider(config.token());
        }
        this.transport = GrpcTransport.forConnectionString(config.connectionString())
                .withAuthProvider(provider)
                .build();
        this.queryClient = QueryClient.newClient(transport).build();
        // Align the retry budget with the JDBC client so dashboards comparing
        // the two implementations measure comparable retry pressure under
        // chaos.
        this.retryCtx = SessionRetryContext.create(queryClient)
                .maxRetries(Math.max(1, params.maxAttempts()))
                .build();
    }

    @Override
    public void createTable(KvWorkloadParams params, String tablePath) {
        Status status = retryCtx.supplyResult(session ->
                session.createQuery(
                        String.format(
                                KvSchema.CREATE_TABLE_TEMPLATE,
                                tablePath,
                                params.minPartitionCount(),
                                params.partitionSizeMb(),
                                params.minPartitionCount(),
                                params.maxPartitionCount()
                        ),
                        TxMode.NONE
                ).execute()
        ).join().getStatus();
        status.expectSuccess("failed to create table " + tablePath);
    }

    @Override
    public void dropTable(String tablePath) {
        Status status = retryCtx.supplyResult(session ->
                session.createQuery(
                        String.format(KvSchema.DROP_TABLE_TEMPLATE, tablePath),
                        TxMode.NONE
                ).execute()
        ).join().getStatus();
        if (!status.isSuccess()) {
            org.slf4j.LoggerFactory.getLogger(QueryKvClient.class)
                    .warn("failed to drop table {}: {}", tablePath, status);
        }
    }

    @Override
    public KvSession openSession() {
        return new QueryKvSession(retryCtx, tablePath);
    }

    @Override
    public void close() {
        try {
            queryClient.close();
        } catch (Exception ignored) {
            // best-effort
        }
        try {
            transport.close();
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private static final class QueryKvSession implements KvSession {
        private final SessionRetryContext retryCtx;
        private final String tablePath;

        private QueryKvSession(SessionRetryContext retryCtx, String tablePath) {
            this.retryCtx = retryCtx;
            this.tablePath = tablePath;
        }

        @Override
        public OpOutcome read(long id, int timeoutMs) {
            long hash = RowGenerator.numericHash(id);
            AtomicInteger attempts = new AtomicInteger();
            ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                    .withRequestTimeout(Duration.ofMillis(timeoutMs))
                    .build();

            Result<QueryReader> result = retryCtx.supplyResult(session -> {
                attempts.incrementAndGet();
                return QueryReader.readFrom(session.createQuery(
                        String.format(READ_QUERY_TEMPLATE, tablePath),
                        TxMode.SNAPSHOT_RO,
                        Params.of(
                                "$id", PrimitiveValue.newUint64(id),
                                "$hash", PrimitiveValue.newUint64(hash)
                        ),
                        settings
                ));
            }).join();

            int retryAttempts = Math.max(0, attempts.get() - 1);
            if (!result.getStatus().isSuccess()) {
                return OpOutcome.error(retryAttempts, classifyStatus(result.getStatus()));
            }

            QueryReader reader = result.getValue();
            if (reader.getResultSetCount() > 0) {
                ResultSetReader rs = reader.getResultSet(0);
                while (rs.next()) {
                    rs.getColumn("id").getUint64();
                }
            }
            return OpOutcome.success(retryAttempts);
        }

        @Override
        public OpOutcome write(Row row, int timeoutMs) {
            long hash = RowGenerator.numericHash(row.id());
            AtomicInteger attempts = new AtomicInteger();
            ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                    .withRequestTimeout(Duration.ofMillis(timeoutMs))
                    .build();

            Status status = retryCtx.supplyStatus(session -> {
                attempts.incrementAndGet();
                return session.createQuery(
                        String.format(WRITE_QUERY_TEMPLATE, tablePath),
                        TxMode.SERIALIZABLE_RW,
                        Params.of(
                                "$hash", PrimitiveValue.newUint64(hash),
                                "$id", PrimitiveValue.newUint64(row.id()),
                                "$payload_str", PrimitiveValue.newText(row.payloadStr()),
                                "$payload_double", PrimitiveValue.newDouble(row.payloadDouble()),
                                "$payload_timestamp", PrimitiveValue.newTimestamp(row.payloadTimestamp()),
                                "$payload_hash", PrimitiveValue.newUint64(row.payloadHash())
                        ),
                        settings
                ).execute().thenApply(Result::getStatus);
            }).join();

            int retryAttempts = Math.max(0, attempts.get() - 1);
            if (status.isSuccess()) {
                return OpOutcome.success(retryAttempts);
            }
            return OpOutcome.error(retryAttempts, classifyStatus(status));
        }
    }

    private static String classifyStatus(Status status) {
        return "ydb/" + status.getCode().name().toLowerCase();
    }
}
