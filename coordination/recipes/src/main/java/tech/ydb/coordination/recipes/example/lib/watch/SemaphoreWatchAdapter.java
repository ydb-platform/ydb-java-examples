package tech.ydb.coordination.recipes.example.lib.watch;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.description.SemaphoreDescription;
import tech.ydb.coordination.description.SemaphoreWatcher;
import tech.ydb.coordination.settings.DescribeSemaphoreMode;
import tech.ydb.coordination.settings.WatchSemaphoreMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;

public class SemaphoreWatchAdapter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SemaphoreWatchAdapter.class);

    private final CoordinationSession session;
    private final String semaphoreName;

    private AtomicReference<State> state;
    private Future<Void> watchTask;
    private volatile WatchData watchData;

    private enum State {
        CREATED,
        STARTED,
        CLOSED
    }

    private class WatchData {
        final long count;
        final byte[] data;
        final List<Participant> waiters;
        final List<Participant> owners;
        final List<Participant> participants;

        WatchData(long count, byte[] data, List<Participant> waiters, List<Participant> owners) {
            this.count = count;
            this.data = data;
            this.waiters = waiters;
            this.owners = owners;
            this.participants = Stream.concat(owners.stream(), waiters.stream()).collect(Collectors.toList());
        }
    }

    public SemaphoreWatchAdapter(CoordinationSession session, String semaphoreName) {
        this.session = session;
        this.semaphoreName = semaphoreName;
        this.state = new AtomicReference<>(State.CREATED);
        this.watchTask = null;
        this.watchData = null;
    }

    public List<Participant> getOwners() {
        // TODO: block until initialized or throw exception or return default value or return Optional.empty()
        Preconditions.checkState(watchData == null, "Is not yet fetched state");

        return Collections.unmodifiableList(watchData.owners); // TODO: copy Participant.data[]?
    }

    public List<Participant> getWaiters() {
        Preconditions.checkState(watchData == null, "Is not yet fetched state");

        return Collections.unmodifiableList(watchData.waiters); // TODO: copy Participant.data[]?
    }

    public List<Participant> getParticipants() {
        Preconditions.checkState(watchData == null, "Is not yet fetched state");

        return Collections.unmodifiableList(watchData.participants); // TODO: copy Participant.data[]?
    }

    public long getCount() {
        Preconditions.checkState(watchData == null, "Is not yet fetched state");

        return watchData.count;
    }

    public byte[] getData() {
        Preconditions.checkState(watchData == null, "Is not yet fetched state");

        return watchData.data.clone();
    }

    public boolean start() {
        Preconditions.checkState(state.compareAndSet(State.CREATED, State.STARTED), "Already started or closed");

        return enqueueWatch();
    }

    private synchronized boolean enqueueWatch() {
        if (watchIsQueued() && state.get() == State.STARTED) {
            return false;
        }

        watchTask = watchSemaphore().thenCompose(status -> {
            if (!status.isSuccess()) {
                // TODO: stop watching on error?
                logger.error("Wailed to watch semaphore: {} with status: {}", semaphoreName, status);
            }

            finish();
            return null;
        });
        return true;
    }

    private boolean watchIsQueued() {
        return watchTask != null;
    }

    private synchronized void finish() {
        watchTask = null;
        enqueueWatch();
    }

    private CompletableFuture<Status> watchSemaphore() {
        return session.watchSemaphore(
                semaphoreName,
                DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS,
                WatchSemaphoreMode.WATCH_DATA_AND_OWNERS
        ).thenCompose(result -> {
            Status status = result.getStatus();
            if (!status.isSuccess()) {
                return CompletableFuture.completedFuture(status);
            }
            SemaphoreWatcher watcher = result.getValue();
            saveWatchState(watcher.getDescription());
            return watcher.getChangedFuture().thenApply(Result::getStatus);
        });
    }

    private void saveWatchState(SemaphoreDescription description) {
        List<Participant> waitersList = description.getWaitersList().stream().map(it -> new Participant(
                it.getId(),
                it.getData(),
                it.getCount(),
                false
        )).collect(Collectors.toList());
        List<Participant> ownersList = description.getOwnersList().stream().map(it -> new Participant(
                it.getId(),
                it.getData(),
                it.getCount(),
                true
        )).collect(Collectors.toList());

        watchData = new WatchData(
                description.getCount(),
                description.getData(),
                waitersList,
                ownersList
        );
    }

    private synchronized void stopWatch() {
        Future<Void> task = watchTask;
        if (task != null) {
            task.cancel(true);
        }
        watchTask = null;
    }

    @Override
    public void close() {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Is not yet started");

        stopWatch();
    }
}
