package tech.ydb.slo.core.rmw;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Bounded concurrent rounds: a fast writer cannot starve a retrying peer. */
public final class RmwRunner {
    public interface Client extends AutoCloseable {
        void create();
        CompletableFuture<Void> apply(RmwState.Operation operation);
        RmwState.Snapshot read();
        void drop();
        @Override
        void close();
    }

    private final Client client;
    private final int writers;
    private final LongSupplier clock;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public RmwRunner(Client client, int writers) {
        this(client, writers, System::nanoTime);
    }

    RmwRunner(Client client, int writers, LongSupplier clock) {
        if (writers < 2) {
            throw new IllegalArgumentException("At least two contending writers are required");
        }
        this.client = client;
        this.writers = writers;
        this.clock = clock;
    }

    public void requestStop() {
        stop.set(true);
    }

    public Throwable failure() {
        return failure.get();
    }

    public int run(Duration duration) {
        Map<String, RmwState.Operation> expected = new LinkedHashMap<>();
        try {
            client.create();
            long started = clock.getAsLong();
            long round = 0;
            while (!stop.get() && (duration.isZero()
                    || clock.getAsLong() - started < duration.toNanos())) {
                List<CompletableFuture<Void>> pending = new ArrayList<>();
                for (int writer = 0; writer < writers && !stop.get(); writer++) {
                    RmwState.Operation operation = new RmwState.Operation(
                            writer + ":" + round, round % 2, writer + 1L);
                    // Retain the independent expected image BEFORE invoking the adapter.
                    expected.put(operation.id(), operation);
                    try {
                        pending.add(client.apply(operation).handle((ignored, error) -> {
                            if (error != null) {
                                fail(error);
                            }
                            return null;
                        }));
                    } catch (Throwable error) {
                        fail(error);
                    }
                }
                // Planned stop and final errors stop admissions, not admitted SDK operations.
                CompletableFuture.allOf(pending.toArray(new CompletableFuture<?>[0])).join();
                round++;
            }
            // Independent read after every admitted operation has settled, even after failure.
            RmwState.validate(expected, client.read());
        } catch (Throwable error) {
            fail(error);
        } finally {
            try {
                client.drop();
            } catch (Throwable error) {
                fail(error);
            }
            try {
                client.close();
            } catch (Throwable error) {
                fail(error);
            }
        }
        return failure.get() == null ? 0 : 1;
    }

    private void fail(Throwable error) {
        while (error instanceof CompletionException && error.getCause() != null) {
            error = error.getCause();
        }
        failure.compareAndSet(null, error);
        stop.set(true);
    }
}
