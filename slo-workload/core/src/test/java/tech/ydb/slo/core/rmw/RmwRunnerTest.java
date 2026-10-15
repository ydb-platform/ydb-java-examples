package tech.ydb.slo.core.rmw;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RmwRunnerTest {
    @Test
    void plannedStopSettlesInflightAndValidatesBeforeDropping() throws Exception {
        ControlledClient client = new ControlledClient();
        RmwRunner runner = new RmwRunner(client, 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> result = executor.submit(() -> runner.run(Duration.ZERO));
            client.rounds.get(0).get(10, TimeUnit.SECONDS);
            runner.requestStop();
            assertFalse(result.isDone());
            assertFalse(client.read);
            client.succeed(0);
            assertFalse(result.isDone());
            client.succeed(1);
            assertEquals(0, result.get(10, TimeUnit.SECONDS));
            assertNull(runner.failure());
            assertTrue(client.read);
            assertTrue(client.dropped);
            assertTrue(client.closed);
            assertEquals(List.of(new RmwState.Operation("0:0", 0, 1),
                    new RmwState.Operation("1:0", 0, 2)), client.admitted);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void boundedRoundsGiveEveryWriterOneAdmissionBeforeAnyNextRound() throws Exception {
        ControlledClient client = new ControlledClient();
        RmwRunner runner = new RmwRunner(client, 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> result = executor.submit(() -> runner.run(Duration.ZERO));
            client.rounds.get(0).get(10, TimeUnit.SECONDS);
            client.succeed(0);
            assertEquals(2, client.admitted.size());
            assertFalse(client.rounds.get(1).isDone());
            client.succeed(1);
            client.rounds.get(1).get(10, TimeUnit.SECONDS);
            runner.requestStop();
            client.succeed(3);
            client.succeed(2);
            assertEquals(0, result.get(10, TimeUnit.SECONDS));
            assertEquals(List.of("0:0", "1:0", "0:1", "1:1"),
                    client.admitted.stream().map(RmwState.Operation::id).toList());
            assertEquals(List.of(0L, 0L, 1L, 1L),
                    client.admitted.stream().map(RmwState.Operation::counter).toList());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void firstFinalCauseSurvivesPeerSettlementValidationAndCleanupFailures() throws Exception {
        ControlledClient client = new ControlledClient() {
            @Override
            public void drop() {
                super.drop();
                throw new IllegalStateException("cleanup");
            }
        };
        RuntimeException nativeCause = new RuntimeException("native final cause");
        RmwRunner runner = new RmwRunner(client, 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> result = executor.submit(() -> runner.run(Duration.ZERO));
            client.rounds.get(0).get(10, TimeUnit.SECONDS);
            client.pending.get(0).completeExceptionally(nativeCause);
            assertFalse(result.isDone());
            client.succeed(1);
            assertEquals(1, result.get(10, TimeUnit.SECONDS));
            assertSame(nativeCause, runner.failure());
            assertTrue(client.read);
            assertTrue(client.dropped);
            assertTrue(client.closed);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void ordinaryDurationCompletesHealthyRound() {
        AtomicLong time = new AtomicLong();
        ControlledClient client = new ControlledClient() {
            @Override
            public CompletableFuture<Void> apply(RmwState.Operation operation) {
                applied.add(operation);
                return CompletableFuture.completedFuture(null);
            }
        };
        RmwRunner runner = new RmwRunner(client, 2, () -> time.getAndAdd(1_000_000_000L));
        assertEquals(0, runner.run(Duration.ofSeconds(2)));
        assertEquals(2, client.applied.size());
        assertTrue(client.read);
        assertTrue(client.dropped);
    }

    @Test
    void coherentLossFailsAgainstPreAdmissionImage() {
        AtomicLong time = new AtomicLong();
        ControlledClient client = new ControlledClient() {
            @Override
            public CompletableFuture<Void> apply(RmwState.Operation operation) {
                // Report success but lose BOTH ledger and matching counter update.
                return CompletableFuture.completedFuture(null);
            }
        };
        RmwRunner runner = new RmwRunner(client, 2, () -> time.getAndAdd(1_000_000_000L));
        assertEquals(1, runner.run(Duration.ofSeconds(2)));
        assertInstanceOf(IllegalStateException.class, runner.failure());
        assertTrue(client.dropped);
    }

    @Test
    void setupFailureStillDropsAndClosesWithOriginalCause() {
        RuntimeException original = new RuntimeException("create");
        ControlledClient client = new ControlledClient() {
            @Override
            public void create() {
                throw original;
            }
        };
        RmwRunner runner = new RmwRunner(client, 2);
        assertEquals(1, runner.run(Duration.ofSeconds(1)));
        assertSame(original, runner.failure());
        assertTrue(client.dropped);
        assertTrue(client.closed);
    }

    private static class ControlledClient implements RmwRunner.Client {
        final List<RmwState.Operation> admitted = new ArrayList<>();
        final List<RmwState.Operation> applied = new ArrayList<>();
        final List<CompletableFuture<Void>> pending = new ArrayList<>();
        final List<CompletableFuture<Void>> rounds = List.of(new CompletableFuture<>(), new CompletableFuture<>());
        boolean read;
        boolean dropped;
        boolean closed;

        public void create() { }

        public CompletableFuture<Void> apply(RmwState.Operation operation) {
            admitted.add(operation);
            CompletableFuture<Void> future = new CompletableFuture<>();
            pending.add(future);
            if (pending.size() % 2 == 0) {
                rounds.get(pending.size() / 2 - 1).complete(null);
            }
            return future;
        }

        void succeed(int index) {
            applied.add(admitted.get(index));
            pending.get(index).complete(null);
        }

        public RmwState.Snapshot read() {
            read = true;
            long[] values = new long[2];
            long[] versions = new long[2];
            for (RmwState.Operation operation : applied) {
                values[(int) operation.counter()] += operation.delta();
                versions[(int) operation.counter()]++;
            }
            return new RmwState.Snapshot(List.of(new RmwState.Counter(0, values[0], versions[0]),
                    new RmwState.Counter(1, values[1], versions[1])), List.copyOf(applied));
        }

        public void drop() {
            assertTrue(pending.stream().allMatch(CompletableFuture::isDone));
            dropped = true;
        }

        public void close() {
            closed = true;
        }
    }
}
