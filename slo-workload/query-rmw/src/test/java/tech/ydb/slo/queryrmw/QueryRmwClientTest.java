package tech.ydb.slo.queryrmw;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.slo.core.Metrics;
import tech.ydb.slo.core.TestMetrics;
import tech.ydb.slo.core.rmw.RmwRunner;
import tech.ydb.slo.core.rmw.RmwState;

import static org.junit.jupiter.api.Assertions.*;

class QueryRmwClientTest {
    private final RmwState.Operation operation = new RmwState.Operation("0:0", 0, 3);

    @Test
    void actualQueryAdapterCreatesAppliesReadsAndDropsPrivateTables() throws Exception {
        QueryBoundary boundary = new QueryBoundary();
        try (Metrics metrics = TestMetrics.create();
                QueryRmwClient client = new QueryRmwClient(boundary, "private", metrics)) {
            client.create();
            client.apply(operation).get(10, TimeUnit.SECONDS);
            RmwState.Operation other = new RmwState.Operation("1:0", 1, 7);
            client.apply(other).get(10, TimeUnit.SECONDS);
            RmwState.Snapshot snapshot = client.read();
            RmwState.validate(Map.of(operation.id(), operation, other.id(), other), snapshot);
            assertEquals(List.of(new RmwState.Counter(0, 3, 1), new RmwState.Counter(1, 7, 1)), snapshot.counters());
            client.drop();
            assertEquals(5, boundary.schema.size());
            assertTrue(boundary.schema.get(0).startsWith("CREATE TABLE `private_counters`"));
            assertTrue(boundary.schema.get(1).startsWith("CREATE TABLE `private_operations`"));
            assertEquals("DROP TABLE `private_operations`;", boundary.schema.get(3));
            assertEquals("DROP TABLE `private_counters`;", boundary.schema.get(4));
            assertEquals(boundary.acquisitions, boundary.sessionsClosed);
        }
        assertTrue(boundary.closed);
    }

    @Test
    void failedCreateDropsOnlyAcknowledgedTables() {
        for (String table : List.of("private_operations", "private_counters")) {
            QueryBoundary boundary = new QueryBoundary();
            UnexpectedResultException cause = new UnexpectedResultException("create rejected",
                    Status.of(StatusCode.SCHEME_ERROR));
            boundary.failedSchema = "CREATE TABLE `" + table + "`";
            boundary.schemaException = cause;
            try (Metrics metrics = TestMetrics.create()) {
                RmwRunner runner = new RmwRunner(new QueryRmwClient(boundary, "private", metrics), 2);
                assertEquals(1, runner.run(Duration.ofSeconds(1)));
                assertSame(cause, runner.failure());
                assertFalse(boundary.schema.contains("DROP TABLE `" + table + "`;"));
                assertEquals(table.equals("private_operations"),
                        boundary.schema.contains("DROP TABLE `private_counters`;"));
                assertTrue(boundary.closed);
            }
        }
    }

    @Test
    void realSdkRetriesAcquisitionAndCommitAndObservationCountsBoth() throws Exception {
        QueryBoundary boundary = new QueryBoundary();
        boundary.acquisitionStatus = StatusCode.BAD_SESSION;
        boundary.commitStatus = StatusCode.BAD_SESSION;
        AttemptClient observed = new AttemptClient(boundary);
        try (Metrics metrics = TestMetrics.create();
                QueryRmwClient client = new QueryRmwClient(observed, "private", metrics)) {
            client.apply(operation).get(10, TimeUnit.SECONDS);
            assertEquals(3, observed.attempts());
            assertEquals(3, boundary.acquisitions);
            assertEquals(2, boundary.sessionsClosed);
            assertEquals(List.of(operation.id(), operation.id()), boundary.idsRead);
            assertEquals(2, boundary.writes);
            assertEquals(2, boundary.commits);
            assertEquals(Map.of(operation.id(), operation), boundary.ledger);
            assertEquals(new RmwState.Counter(0, 3, 1), boundary.counters[0]);
            assertSame(boundary.getScheduler(), observed.getScheduler());
            assertSame(boundary.getTracer(), observed.getTracer());
        } finally {
            boundary.close();
        }
    }

    @Test
    void ambiguousCommitUsesSameIdAndDeduplicatesThroughActualSdkRetry() throws Exception {
        QueryBoundary boundary = new QueryBoundary();
        boundary.commitStatus = StatusCode.UNDETERMINED;
        AttemptClient observed = new AttemptClient(boundary);
        try (Metrics metrics = TestMetrics.create();
                QueryRmwClient client = new QueryRmwClient(observed, "private", metrics)) {
            client.apply(operation).get(10, TimeUnit.SECONDS);
            assertEquals(2, observed.attempts());
            assertEquals(List.of(operation.id(), operation.id()), boundary.idsRead);
            assertEquals(1, boundary.writes);
            assertEquals(2, boundary.commits);
            assertEquals(new RmwState.Counter(0, 3, 1), boundary.counters[0]);
            client.apply(operation).get(10, TimeUnit.SECONDS);
            assertEquals(1, boundary.writes);
            assertEquals(1, boundary.ledger.size());
            RmwState.validate(Map.of(operation.id(), operation), client.read());
        } finally {
            boundary.close();
        }
    }

    @Test
    void finalNativeCommitCauseSurvivesLifecycleAndMissingLedger() {
        QueryBoundary boundary = new QueryBoundary();
        UnexpectedResultException nativeCause = new UnexpectedResultException("commit rejected", Status.of(StatusCode.BAD_REQUEST));
        boundary.commitException = nativeCause;
        try (Metrics metrics = TestMetrics.create()) {
            QueryRmwClient client = new QueryRmwClient(boundary, "private", metrics);
            RmwRunner runner = new RmwRunner(client, 2);
            assertEquals(1, runner.run(Duration.ofSeconds(1)));
            assertSame(nativeCause, runner.failure());
            assertEquals(1, boundary.snapshots);
            assertEquals(1, boundary.commits);
            assertTrue(boundary.closed);
            assertTrue(boundary.ledger.isEmpty());
            assertEquals("DROP TABLE `private_counters`;", boundary.schema.get(boundary.schema.size() - 1));
        }
    }

    @Test
    void nativeStreamFailureIsNotReplacedByMissingRows() throws Exception {
        QueryBoundary boundary = new QueryBoundary();
        UnexpectedResultException nativeCause = new UnexpectedResultException("stream rejected", Status.of(StatusCode.BAD_REQUEST));
        boundary.readException = nativeCause;
        try (Metrics metrics = TestMetrics.create();
                QueryRmwClient client = new QueryRmwClient(boundary, "private", metrics)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> client.apply(operation).get(10, TimeUnit.SECONDS));
            assertSame(nativeCause, failure.getCause());
            assertEquals(1, boundary.acquisitions);
            assertEquals(0, boundary.commits);
        }
    }

    @Test
    void dedupPayloadMismatchIsTerminalAndDoesNotRetryOrApply() {
        QueryBoundary boundary = new QueryBoundary();
        boundary.ledger.put(operation.id(), new RmwState.Operation(operation.id(), 1, operation.delta()));
        try (Metrics metrics = TestMetrics.create();
                QueryRmwClient client = new QueryRmwClient(boundary, "private", metrics)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> client.apply(operation).get(10, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertEquals(1, boundary.acquisitions);
            assertEquals(0, boundary.writes);
            assertEquals(0, boundary.commits);
        }
    }

    @Test
    void successfulNativeSnapshotWithWrongVersionFailsWithoutRetryingRead() {
        QueryBoundary boundary = new QueryBoundary();
        boundary.ledger.put(operation.id(), operation);
        boundary.counters[0] = new RmwState.Counter(0, 3, 0);
        try (Metrics metrics = TestMetrics.create();
                QueryRmwClient client = new QueryRmwClient(boundary, "private", metrics)) {
            RmwState.Snapshot snapshot = client.read();
            assertThrows(IllegalStateException.class,
                    () -> RmwState.validate(Map.of(operation.id(), operation), snapshot));
            assertEquals(1, boundary.acquisitions);
            assertEquals(1, boundary.snapshots);
        }
    }
}
