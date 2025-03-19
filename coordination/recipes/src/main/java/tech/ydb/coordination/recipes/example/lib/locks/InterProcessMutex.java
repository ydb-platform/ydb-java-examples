package tech.ydb.coordination.recipes.example.lib.locks;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.SemaphoreLease;
import tech.ydb.coordination.description.SemaphoreDescription;
import tech.ydb.coordination.recipes.example.lib.util.SessionListenerWrapper;
import tech.ydb.coordination.recipes.example.lib.util.Listenable;
import tech.ydb.coordination.recipes.example.lib.util.ListenableProvider;
import tech.ydb.coordination.settings.DescribeSemaphoreMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;

@ThreadSafe
public class InterProcessMutex implements InterProcessLock, ListenableProvider<CoordinationSession.State> {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final Logger logger = LoggerFactory.getLogger(InterProcessMutex.class);

    private final Lock leaseLock = new ReentrantLock();
    private final CoordinationSession session;
    private final CompletableFuture<Status> sessionConnectionTask;
    private final SessionListenerWrapper sessionListenerWrapper;
    private final String semaphoreName;
    private final String coordinationNodePath;

    private volatile SemaphoreLease processLease = null;

    public InterProcessMutex(
            CoordinationClient client,
            String coordinationNodePath,
            String lockName
    ) {
        this.coordinationNodePath = coordinationNodePath;
        this.session = client.createSession(coordinationNodePath);
        this.sessionListenerWrapper = new SessionListenerWrapper(session);
        this.semaphoreName = lockName;

        this.sessionConnectionTask = session.connect().thenApply(status -> {
            logger.debug("Session connection status: " + status);
            return status;
        });
        session.addStateListener(state -> {
            switch (state) {
                case RECONNECTED: {
                    logger.debug("Session RECONNECTED");
                    reconnect();
                    break;
                }
                case CLOSED: {
                    logger.debug("Session CLOSED, releasing lock");
                    internalRelease();
                    break;
                }
                case LOST: {
                    logger.debug("Session LOST, releasing lock");
                    internalRelease();
                    break;
                }
            }
        });
    }

    private CoordinationSession connectedSession() {
        try {
            sessionConnectionTask.get().expectSuccess("Unable to connect to session on: " + coordinationNodePath);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return session;
    }

    private void reconnect() {
        connectedSession().describeSemaphore(
                semaphoreName,
                DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS
        ).thenAccept(result -> {
            if (!result.isSuccess()) {
                logger.error("Unable to describe semaphore {}", semaphoreName);
                return;
            }
            SemaphoreDescription semaphoreDescription = result.getValue();
            SemaphoreDescription.Session owner = semaphoreDescription.getOwnersList().getFirst();
            if (owner.getId() != session.getId()) {
                logger.warn(
                        "Current session with id: {} lost lease after reconnection on semaphore: {}",
                        owner.getId(),
                        semaphoreName
                );
                internalRelease();
            }
        });
    }

    @Override
    public void acquire() throws Exception {
        logger.debug("Trying to acquire without timeout");
        safeAcquire(null);
    }

    @Override
    public boolean acquire(Duration duration) throws Exception {
        logger.debug("Trying to acquire with deadline: {}", duration);
        Instant deadline = Instant.now().plus(duration);
        return safeAcquire(deadline);
    }

    @Override
    public boolean release() throws Exception {
        return internalRelease().get();
    }

    private CompletableFuture<Boolean> internalRelease() {
        logger.debug("Trying to release");
        if (processLease == null) {
            logger.debug("Already released");
            return CompletableFuture.completedFuture(false);
        }

        leaseLock.lock();
        try {
            if (processLease != null) {
                return processLease.release().thenApply(it -> {
                    logger.debug("Released lock");
                    processLease = null;
                    leaseLock.unlock();
                    return true;
                });
            }
        } finally {
            leaseLock.unlock();
        }

        logger.debug("Already released");
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public boolean isAcquiredInThisProcess() {
        return processLease != null;
    }

    // TODO: implement interruption

    /**
     * @param deadline
     * @return true - if successfully acquired lock
     * @throws Exception
     * @throws LockAlreadyAcquiredException
     */
    private boolean safeAcquire(@Nullable Instant deadline) throws Exception, LockAlreadyAcquiredException {
        if (processLease != null) {
            logger.debug("Already acquired lock: {}", semaphoreName);
            throw new LockAlreadyAcquiredException(semaphoreName);
        }

        leaseLock.lock();
        try {
            if (processLease != null) {
                logger.debug("Already acquired lock: {}", semaphoreName);
                throw new LockAlreadyAcquiredException(semaphoreName);
            }

            SemaphoreLease lease = internalLock(deadline);
            if (lease != null) {
                processLease = lease;
                logger.debug("Successfully acquired lock: {}", semaphoreName);
                return true;
            }
        } finally {
            leaseLock.unlock();
        }

        logger.debug("Unable to acquire lock: {}", semaphoreName);
        return false;
    }

    private SemaphoreLease internalLock(@Nullable Instant deadline) throws ExecutionException, InterruptedException {
        int retryCount = 0;
        while (connectedSession().getState().isActive() && (deadline == null || Instant.now().isBefore(deadline))) {
            retryCount++;

            Duration timeout;
            if (deadline == null) {
                timeout = DEFAULT_TIMEOUT;
            } else {
                timeout = Duration.between(Instant.now(), deadline); // TODO: use external Clock instead of Instant?
            }
            CompletableFuture<Result<SemaphoreLease>> acquireTask = connectedSession().acquireEphemeralSemaphore(
                    semaphoreName, true, null, timeout // TODO: change Session API to use deadlines
            );
            Result<SemaphoreLease> leaseResult;
            try {
                leaseResult = acquireTask.get();
            } catch (InterruptedException e) {
                // If acquire is interrupted, then release immediately
                Thread.currentThread().interrupt();
                acquireTask.thenAccept(acquireResult -> {
                    if (!acquireResult.getStatus().isSuccess()) {
                        return;
                    }
                    SemaphoreLease lease = acquireResult.getValue();
                    lease.release();
                });
                throw e;
            }

            Status status = leaseResult.getStatus();
            logger.debug("Lease result status: {}", status);

            if (status.isSuccess()) {
                logger.debug("Successfully acquired the lock");
                return leaseResult.getValue();
            }

            if (status.getCode() == StatusCode.TIMEOUT) {
                logger.debug("Trying to acquire again, retries: {}", retryCount);
                continue;
            }

            if (!status.getCode().isRetryable(true)) {
                status.expectSuccess("Unable to retry acquiring semaphore");
                return null;
            }
        }

        // TODO: handle timeout and error differently
        throw new LockAcquireFailedException(coordinationNodePath, semaphoreName);
    }

    @Override
    public Listenable<CoordinationSession.State> getListenable() {
        return sessionListenerWrapper;
    }

    public CoordinationSession getSession() {
        return session;
    }
}
