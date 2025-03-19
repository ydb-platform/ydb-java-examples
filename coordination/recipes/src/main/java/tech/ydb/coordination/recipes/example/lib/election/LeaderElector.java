package tech.ydb.coordination.recipes.example.lib.election;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.recipes.example.lib.watch.Participant;
import tech.ydb.coordination.recipes.example.lib.locks.InterProcessMutex;
import tech.ydb.coordination.recipes.example.lib.watch.SemaphoreWatchAdapter;
import tech.ydb.coordination.recipes.example.lib.util.Listenable;
import tech.ydb.coordination.recipes.example.lib.util.ListenableProvider;

public class LeaderElector implements Closeable, ListenableProvider<CoordinationSession.State> {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElector.class);

    private final CoordinationClient client;
    private final LeaderElectionListener leaderElectionListener;
    private final String coordinationNodePath;
    private final String semaphoreName;
    private final ExecutorService electionExecutor;
    private final InterProcessMutex lock;
    private final SemaphoreWatchAdapter semaphoreWatchAdapter;

    private AtomicReference<State> state = new AtomicReference<>(State.STARTED);
    private volatile boolean autoRequeue = false;
    private volatile boolean isLeader = false;
    private Future<Void> electionTask = null;


    private enum State { // TODO: needs third state (CREATED)?
        STARTED,
        CLOSED
    }

    public LeaderElector(
            CoordinationClient client,
            LeaderElectionListener leaderElectionListener,
            String coordinationNodePath,
            String semaphoreName
    ) {
        this(client, leaderElectionListener, coordinationNodePath, semaphoreName, Executors.newSingleThreadExecutor());
    }

    public LeaderElector(
            CoordinationClient client,
            LeaderElectionListener leaderElectionListener,
            String coordinationNodePath,
            String semaphoreName,
            ExecutorService executorService
    ) {
        this.client = client;
        this.leaderElectionListener = leaderElectionListener;
        this.coordinationNodePath = coordinationNodePath;
        this.semaphoreName = semaphoreName;
        this.electionExecutor = executorService;
        this.lock = new InterProcessMutex(
                client,
                coordinationNodePath,
                semaphoreName
        );
        this.semaphoreWatchAdapter = new SemaphoreWatchAdapter(lock.getSession(), semaphoreName);
        semaphoreWatchAdapter.start();
    }

    public boolean isLeader() {
        return isLeader;
    }

    public synchronized void interruptLeadership() {
        Future<?> task = electionTask;
        if (task != null) {
            task.cancel(true);
        }
    }

    /**
     * Re-queue an attempt for leadership. If this instance is already queued, nothing
     * happens and false is returned. If the instance was not queued, it is re-queued and true
     * is returned
     *
     * @return true if re-enqueue was successful
     */
    public boolean requeue() {
        Preconditions.checkState(state.get() == State.STARTED, "Already closed or not yet started");

        return enqueueElection();
    }

    public void autoRequeue() {
        autoRequeue = true;
    }

    private synchronized boolean enqueueElection() {
        if (!isQueued() && state.get() == State.STARTED) {
            electionTask = electionExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        doWork();
                    } finally {
                        finishTask();
                    }
                    return null;
                }
            });
            return true;
        }

        return false;
    }

    private void doWork() throws Exception {
        isLeader = false;

        try {
            lock.acquire();
            isLeader = true;
            try {
                leaderElectionListener.takeLeadership();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Throwable e) {
                logger.debug("takeLeadership exception", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            if (isLeader) {
                isLeader = false;
                boolean wasInterrupted = Thread.interrupted();
                try {
                    lock.release();
                } catch (Exception e) {
                    logger.error("Lock release exception for: " + coordinationNodePath);
                } finally {
                    if (wasInterrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private synchronized void finishTask() {
        electionTask = null;
        if (autoRequeue) { // TODO: requeue if critical exception?
            enqueueElection();
        }
    }

    private boolean isQueued() {
        return electionTask != null;
    }

    public List<Participant> getParticipants() {
        return semaphoreWatchAdapter.getParticipants();
    }

    public Optional<Participant> getLeader() {
        return semaphoreWatchAdapter.getOwners().stream().findFirst();
    }

    @Override
    public synchronized void close() {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed");

        Future<Void> task = electionTask;
        if (task != null) {
            task.cancel(true);
        }

        electionTask = null;
        electionExecutor.close();
        semaphoreWatchAdapter.close();
        getListenable().clearListeners();
    }

    @Override
    public Listenable<CoordinationSession.State> getListenable() {
        return lock.getListenable();
    }
}
