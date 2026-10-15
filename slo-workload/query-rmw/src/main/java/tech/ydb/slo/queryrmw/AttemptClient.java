package tech.ydb.slo.queryrmw;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import tech.ydb.core.Result;
import tech.ydb.core.tracing.Tracer;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.table.SessionPoolStats;

/** Scoped to ONE retry invocation, including failed session acquisition and commit retries. */
final class AttemptClient implements QueryClient {
    private final QueryClient delegate;
    private final AtomicInteger attempts = new AtomicInteger();

    AttemptClient(QueryClient delegate) {
        this.delegate = delegate;
    }

    int attempts() {
        return attempts.get();
    }

    @Override
    public CompletableFuture<Result<QuerySession>> createSession(Duration timeout) {
        attempts.incrementAndGet();
        return delegate.createSession(timeout);
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return delegate.getScheduler();
    }

    @Override
    public Tracer getTracer() {
        return delegate.getTracer();
    }

    @Override
    public SessionPoolStats getSessionPoolStats() {
        return delegate.getSessionPoolStats();
    }

    @Override
    public void close() {
        // This observation view does not own the shared client.
    }
}
