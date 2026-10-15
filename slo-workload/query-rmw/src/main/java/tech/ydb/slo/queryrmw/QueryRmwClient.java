package tech.ydb.slo.queryrmw;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.slo.core.Config;
import tech.ydb.slo.core.Metrics;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.rmw.RmwRunner;
import tech.ydb.slo.core.rmw.RmwState;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

public final class QueryRmwClient implements RmwRunner.Client {
    private final QueryClient client;
    private final GrpcTransport transport;
    private final Metrics metrics;
    private final String counters;
    private final String ledger;
    private final KvWorkloadParams defaults = new KvWorkloadParams();
    private boolean countersCreated;
    private boolean ledgerCreated;

    public QueryRmwClient(Config config, String namespace, Metrics metrics) {
        AuthProvider auth = config.token().isEmpty()
                ? NopAuthProvider.INSTANCE : new TokenAuthProvider(config.token());
        this.transport = GrpcTransport.forConnectionString(config.connectionString())
                .withAuthProvider(auth).build();
        this.client = QueryClient.newClient(transport).build();
        this.metrics = metrics;
        this.counters = namespace + "_counters";
        this.ledger = namespace + "_operations";
    }

    QueryRmwClient(QueryClient client, String namespace, Metrics metrics) {
        this.client = client;
        this.transport = null;
        this.metrics = metrics;
        this.counters = namespace + "_counters";
        this.ledger = namespace + "_operations";
    }

    private <T> CompletableFuture<T> execute(Metrics.OperationType type, boolean idempotent,
            Function<QuerySession, CompletableFuture<Result<T>>> action) {
        AttemptClient observed = new AttemptClient(client);
        Metrics.Span span = metrics.startOperation(type);
        // Same retry budget/backoff/session timeout as the basic Query workload.
        // Only the semantic idempotence declaration differs: the ledger makes RMW replay safe.
        return SessionRetryContext.create(observed).maxRetries(Math.max(1, defaults.maxAttempts()))
                .idempotent(idempotent).build().supplyResult(action)
                .thenApply(Result::getValue).whenComplete((value, error) -> {
                    if (error == null) {
                        span.finishSuccess(observed.attempts());
                    } else {
                        span.finishError(observed.attempts(), error.getClass().getSimpleName());
                    }
                });
    }

    private ExecuteQuerySettings settings(boolean write) {
        return ExecuteQuerySettings.newBuilder().withRequestTimeout(Duration.ofMillis(
                write ? defaults.writeTimeoutMs() : defaults.readTimeoutMs())).build();
    }

    private void schema(String query) {
        execute(Metrics.OperationType.WRITE, false,
                session -> session.createQuery(query, TxMode.NONE, Params.empty(), settings(true)).execute()).join();
    }

    @Override
    public void create() {
        schema("CREATE TABLE `" + counters + "` (id Uint64 NOT NULL, value Int64 NOT NULL, "
                + "version Uint64 NOT NULL, PRIMARY KEY (id));");
        countersCreated = true;
        schema("CREATE TABLE `" + ledger + "` (op_id Utf8 NOT NULL, counter_id Uint64 NOT NULL, "
                + "delta Int64 NOT NULL, PRIMARY KEY (op_id));");
        ledgerCreated = true;
        execute(Metrics.OperationType.WRITE, true, session -> session.createQuery(
                "UPSERT INTO `" + counters + "` (id, value, version) VALUES (0u, 0, 0u), (1u, 0, 0u);",
                TxMode.SERIALIZABLE_RW, Params.empty(), settings(true)).execute()).join();
    }

    @Override
    public CompletableFuture<Void> apply(RmwState.Operation operation) {
        return execute(Metrics.OperationType.WRITE, true, session -> {
            QueryTransaction tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
            return QueryReader.readFrom(tx.createQuery(
                    "DECLARE $op AS Utf8; SELECT op_id, counter_id, delta FROM `" + ledger + "` WHERE op_id = $op;",
                    false, Params.of("$op", PrimitiveValue.newText(operation.id())), settings(true)))
                    .thenCompose(Result.compose(reader -> {
                        List<RmwState.Operation> found = operations(reader);
                        if (!found.isEmpty()) {
                            if (!found.equals(List.of(operation))) {
                                throw new IllegalStateException("Deduplication payload mismatch for " + operation.id());
                            }
                            return tx.commit();
                        }
                        return increment(tx, operation);
                    }));
        }).thenApply(ignored -> null);
    }

    private CompletableFuture<Result<QueryInfo>> increment(QueryTransaction tx, RmwState.Operation operation) {
        return QueryReader.readFrom(tx.createQuery(
                "DECLARE $counter AS Uint64; SELECT id, value, version FROM `" + counters + "` WHERE id = $counter;",
                false, Params.of("$counter", PrimitiveValue.newUint64(operation.counter())), settings(true)))
                .thenCompose(Result.compose(reader -> {
                    List<RmwState.Counter> found = counters(reader);
                    if (found.size() != 1 || found.get(0).id() != operation.counter()) {
                        throw new IllegalStateException("Missing or unexpected counter " + operation.counter());
                    }
                    RmwState.Counter before = found.get(0);
                    Params params = Params.of(
                            "$op", PrimitiveValue.newText(operation.id()),
                            "$counter", PrimitiveValue.newUint64(operation.counter()),
                            "$delta", PrimitiveValue.newInt64(operation.delta()),
                            "$value", PrimitiveValue.newInt64(Math.addExact(before.value(), operation.delta())),
                            "$version", PrimitiveValue.newUint64(Math.incrementExact(before.version())));
                    return tx.createQuery("DECLARE $op AS Utf8; DECLARE $counter AS Uint64; "
                            + "DECLARE $delta AS Int64; DECLARE $value AS Int64; DECLARE $version AS Uint64; "
                            + "UPDATE `" + counters + "` SET value = $value, version = $version WHERE id = $counter; "
                            + "INSERT INTO `" + ledger + "` (op_id, counter_id, delta) VALUES ($op, $counter, $delta);",
                            false, params, settings(true)).execute()
                            .thenCompose(Result.compose(ignored -> tx.commit()));
                }));
    }

    @Override
    public RmwState.Snapshot read() {
        QueryReader reader = execute(Metrics.OperationType.READ, true,
                session -> QueryReader.readFrom(session.createQuery(
                        "SELECT id, value, version FROM `" + counters + "`; "
                                + "SELECT op_id, counter_id, delta FROM `" + ledger + "`;",
                        TxMode.SNAPSHOT_RO, Params.empty(), settings(false)))).join();
        if (reader.getResultSetCount() != 2) {
            throw new IllegalStateException("Expected counter and ledger result sets");
        }
        return new RmwState.Snapshot(counters(reader.getResultSet(0)), operations(reader.getResultSet(1)));
    }

    private static List<RmwState.Counter> counters(QueryReader reader) {
        if (reader.getResultSetCount() != 1) {
            throw new IllegalStateException("Expected one counter result set");
        }
        return counters(reader.getResultSet(0));
    }

    private static List<RmwState.Counter> counters(ResultSetReader rows) {
        List<RmwState.Counter> result = new ArrayList<>();
        while (rows.next()) {
            result.add(new RmwState.Counter(rows.getColumn("id").getUint64(),
                    rows.getColumn("value").getInt64(), rows.getColumn("version").getUint64()));
        }
        return result;
    }

    private static List<RmwState.Operation> operations(QueryReader reader) {
        if (reader.getResultSetCount() != 1) {
            throw new IllegalStateException("Expected one ledger result set");
        }
        return operations(reader.getResultSet(0));
    }

    private static List<RmwState.Operation> operations(ResultSetReader rows) {
        List<RmwState.Operation> result = new ArrayList<>();
        while (rows.next()) {
            result.add(new RmwState.Operation(rows.getColumn("op_id").getText(),
                    rows.getColumn("counter_id").getUint64(), rows.getColumn("delta").getInt64()));
        }
        return result;
    }

    @Override
    public void drop() {
        RuntimeException first = null;
        if (ledgerCreated) {
            try {
                schema("DROP TABLE `" + ledger + "`;");
            } catch (RuntimeException error) {
                first = error;
            }
        }
        if (countersCreated) {
            try {
                schema("DROP TABLE `" + counters + "`;");
            } catch (RuntimeException error) {
                if (first == null) {
                    first = error;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } finally {
            if (transport != null) {
                transport.close();
            }
        }
    }
}
