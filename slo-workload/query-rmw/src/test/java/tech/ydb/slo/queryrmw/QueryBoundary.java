package tech.ydb.slo.queryrmw;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.proto.ValueProtos;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.query.settings.BeginTransactionSettings;
import tech.ydb.query.settings.CommitTransactionSettings;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.settings.RollbackTransactionSettings;
import tech.ydb.slo.core.rmw.RmwState;
import tech.ydb.table.SessionPoolStats;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

import static org.junit.jupiter.api.Assertions.*;

/** SDK boundary only. QueryReader, Result composition and SessionRetryContext remain real. */
final class QueryBoundary implements QueryClient {
    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    final Map<String, RmwState.Operation> ledger = new LinkedHashMap<>();
    final RmwState.Counter[] counters = {new RmwState.Counter(0, 0, 0), new RmwState.Counter(1, 0, 0)};
    final List<String> idsRead = new ArrayList<>();
    final List<String> schema = new ArrayList<>();
    int acquisitions;
    int sessionsClosed;
    int commits;
    int writes;
    int snapshots;
    StatusCode acquisitionStatus;
    StatusCode commitStatus;
    RuntimeException commitException;
    RuntimeException readException;
    String failedSchema;
    RuntimeException schemaException;
    boolean closed;

    public CompletableFuture<Result<QuerySession>> createSession(Duration timeout) {
        assertEquals(Duration.ofSeconds(5), timeout);
        acquisitions++;
        if (acquisitionStatus != null) {
            StatusCode status = acquisitionStatus;
            acquisitionStatus = null;
            return CompletableFuture.completedFuture(Result.fail(Status.of(status)));
        }
        return CompletableFuture.completedFuture(Result.success(new Session()));
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public SessionPoolStats getSessionPoolStats() {
        return null;
    }

    public void close() {
        closed = true;
        scheduler.shutdownNow();
    }

    private final class Session implements QuerySession {
        private QueryTransaction current;
        public String getId() { return "test-session"; }
        public QueryTransaction currentTransaction() { return current; }
        public QueryTransaction createNewTransaction(TxMode mode) {
            assertEquals(TxMode.SERIALIZABLE_RW, mode);
            current = new Transaction(this);
            return current;
        }
        public CompletableFuture<Result<QueryTransaction>> beginTransaction(TxMode mode, BeginTransactionSettings settings) {
            throw new AssertionError("This adapter uses the SDK implicit transaction API");
        }
        public QueryStream createQuery(String sql, TxMode mode, Params params, ExecuteQuerySettings settings) {
            if (mode == TxMode.SNAPSHOT_RO) {
                snapshots++;
                assertTrue(sql.contains("FROM `private_counters`"));
                assertTrue(sql.contains("FROM `private_operations`"));
                return stream(List.of(counterRows(List.of(counters)), operationRows(new ArrayList<>(ledger.values()))));
            }
            schema.add(sql);
            if (failedSchema != null && sql.startsWith(failedSchema)) {
                return failing(schemaException);
            }
            return stream(List.of());
        }
        public void close() { sessionsClosed++; }
    }

    private final class Transaction implements QueryTransaction {
        private final Session session;
        private RmwState.Operation pending;
        private RmwState.Counter updated;
        Transaction(Session session) { this.session = session; }
        public QuerySession getSession() { return session; }
        public String getId() { return "test-tx"; }
        public String getSessionId() { return session.getId(); }
        public TxMode getTxMode() { return TxMode.SERIALIZABLE_RW; }
        public CompletableFuture<Status> getStatusFuture() { return CompletableFuture.completedFuture(Status.SUCCESS); }
        public CompletableFuture<Status> rollback(RollbackTransactionSettings settings) {
            pending = null;
            updated = null;
            return CompletableFuture.completedFuture(Status.SUCCESS);
        }
        public QueryStream createQuery(String sql, boolean commitAtEnd, Params params, ExecuteQuerySettings settings) {
            assertFalse(commitAtEnd);
            if (sql.contains("SELECT op_id")) {
                assertTrue(sql.contains("FROM `private_operations` WHERE op_id = $op"));
                String id = params.toPb().get("$op").getValue().getTextValue();
                idsRead.add(id);
                if (readException != null) {
                    return failing(readException);
                }
                RmwState.Operation operation = ledger.get(id);
                return stream(List.of(operationRows(operation == null ? List.of() : List.of(operation))));
            }
            if (sql.contains("SELECT id")) {
                assertTrue(sql.contains("FROM `private_counters` WHERE id = $counter"));
                int id = (int) params.toPb().get("$counter").getValue().getUint64Value();
                return stream(List.of(counterRows(List.of(counters[id]))));
            }
            assertTrue(sql.contains("UPDATE `private_counters` SET value = $value, version = $version WHERE id = $counter"));
            assertTrue(sql.contains("INSERT INTO `private_operations`"));
            String id = params.toPb().get("$op").getValue().getTextValue();
            long counter = params.toPb().get("$counter").getValue().getUint64Value();
            pending = new RmwState.Operation(id, counter, params.toPb().get("$delta").getValue().getInt64Value());
            updated = new RmwState.Counter(counter, params.toPb().get("$value").getValue().getInt64Value(),
                    params.toPb().get("$version").getValue().getUint64Value());
            writes++;
            return stream(List.of());
        }
        public CompletableFuture<Result<QueryInfo>> commit(CommitTransactionSettings settings) {
            commits++;
            if (commitException != null) {
                return CompletableFuture.failedFuture(commitException);
            }
            StatusCode status = commitStatus;
            commitStatus = null;
            if (status == null || status == StatusCode.UNDETERMINED) {
                if (pending != null) {
                    assertFalse(ledger.containsKey(pending.id()));
                    ledger.put(pending.id(), pending);
                    counters[(int) updated.id()] = updated;
                }
            }
            return CompletableFuture.completedFuture(status == null
                    ? Result.success(new QueryInfo(null)) : Result.fail(Status.of(status)));
        }
    }

    private static QueryStream stream(List<ValueProtos.ResultSet> rows) {
        return new QueryStream() {
            public CompletableFuture<Result<QueryInfo>> execute(PartsHandler handler) {
                if (handler != null) {
                    for (int index = 0; index < rows.size(); index++) {
                        handler.onNextPart(new QueryResultPart(index, rows.get(index)));
                    }
                }
                return CompletableFuture.completedFuture(Result.success(new QueryInfo(null)));
            }
            public void cancel() { throw new AssertionError("No admitted operation may be cancelled"); }
        };
    }

    private static QueryStream failing(RuntimeException error) {
        return new QueryStream() {
            public CompletableFuture<Result<QueryInfo>> execute(PartsHandler handler) {
                return CompletableFuture.failedFuture(error);
            }
            public void cancel() { throw new AssertionError("Unexpected cancellation"); }
        };
    }

    private static ValueProtos.ResultSet counterRows(List<RmwState.Counter> counters) {
        ValueProtos.ResultSet.Builder rows = ValueProtos.ResultSet.newBuilder()
                .addColumns(column("id", PrimitiveType.Uint64))
                .addColumns(column("value", PrimitiveType.Int64))
                .addColumns(column("version", PrimitiveType.Uint64));
        for (RmwState.Counter counter : counters) {
            rows.addRows(ValueProtos.Value.newBuilder().addItems(PrimitiveValue.newUint64(counter.id()).toPb())
                    .addItems(PrimitiveValue.newInt64(counter.value()).toPb())
                    .addItems(PrimitiveValue.newUint64(counter.version()).toPb()));
        }
        return rows.build();
    }

    private static ValueProtos.ResultSet operationRows(List<RmwState.Operation> operations) {
        ValueProtos.ResultSet.Builder rows = ValueProtos.ResultSet.newBuilder()
                .addColumns(column("op_id", PrimitiveType.Text))
                .addColumns(column("counter_id", PrimitiveType.Uint64))
                .addColumns(column("delta", PrimitiveType.Int64));
        for (RmwState.Operation operation : operations) {
            rows.addRows(ValueProtos.Value.newBuilder().addItems(PrimitiveValue.newText(operation.id()).toPb())
                    .addItems(PrimitiveValue.newUint64(operation.counter()).toPb())
                    .addItems(PrimitiveValue.newInt64(operation.delta()).toPb()));
        }
        return rows.build();
    }

    private static ValueProtos.Column column(String name, PrimitiveType type) {
        return ValueProtos.Column.newBuilder().setName(name).setType(type.toPb()).build();
    }
}
