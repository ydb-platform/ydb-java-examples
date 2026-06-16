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
import tech.ydb.slo.core.kv.KvSession;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.OpOutcome;
import tech.ydb.slo.core.kv.Row;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

/**
 * {@link KvClient} backed by the native YDB query client and
 * {@link SessionRetryContext}.
 */
public final class QueryKvClient implements KvClient {
    private static final String CREATE_TABLE_QUERY_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `%s` ("
            + "  hash Uint64,"
            + "  id Uint64,"
            + "  payload_str Utf8,"
            + "  payload_double Double,"
            + "  payload_timestamp Timestamp,"
            + "  payload_hash Uint64,"
            + "  PRIMARY KEY (hash, id)"
            + ") WITH ("
            + "  UNIFORM_PARTITIONS = %d,"
            + "  AUTO_PARTITIONING_BY_SIZE = ENABLED,"
            + "  AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,"
            + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,"
            + "  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d"
            + ")";

    private static final String DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE `%s`";

    private static final String WRITE_QUERY_TEMPLATE = ""
            + "DECLARE $id AS Uint64;"
            + "DECLARE $payload_str AS Utf8;"
            + "DECLARE $payload_double AS Double;"
            + "DECLARE $payload_timestamp AS Timestamp;"
            + "DECLARE $payload_hash AS Uint64;"
            + "UPSERT INTO `%s` ("
            + "  id, hash, payload_str, payload_double, payload_timestamp, payload_hash"
            + ") VALUES ("
            + "  $id,"
            + "  Digest::NumericHash($id),"
            + "  $payload_str,"
            + "  $payload_double,"
            + "  $payload_timestamp,"
            + "  $payload_hash"
            + ");";

    private static final String READ_QUERY_TEMPLATE = ""
            + "DECLARE $id AS Uint64;"
            + "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
            + "  FROM `%s`"
            + "  WHERE id = $id AND hash = Digest::NumericHash($id);";

    private final SessionRetryContext retryCtx;
    private final String tablePath;
    private final GrpcTransport transport;
    private final QueryClient queryClient;

    public QueryKvClient(Config config, String tablePath) {
        this.tablePath = tablePath;
        AuthProvider provider = NopAuthProvider.INSTANCE;
        if (config.token() != null && !config.token().isEmpty()) {
            provider = new TokenAuthProvider(config.token());
        }
        this.transport = GrpcTransport.forConnectionString(config.connectionString())
                .withAuthProvider(provider)
                .build();
        this.queryClient = QueryClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(queryClient).build();
    }

    @Override
    public void createTable(KvWorkloadParams params, String tablePath) {
        Status status = retryCtx.supplyResult(session ->
                session.createQuery(
                        String.format(
                                CREATE_TABLE_QUERY_TEMPLATE,
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
                        String.format(DROP_TABLE_QUERY_TEMPLATE, tablePath),
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
            AtomicInteger attempts = new AtomicInteger();
            ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                    .withRequestTimeout(Duration.ofMillis(timeoutMs))
                    .build();

            Result<QueryReader> result = retryCtx.supplyResult(session -> {
                attempts.incrementAndGet();
                return QueryReader.readFrom(session.createQuery(
                        String.format(READ_QUERY_TEMPLATE, tablePath),
                        TxMode.SNAPSHOT_RO,
                        Params.of("$id", PrimitiveValue.newUint64(id)),
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
