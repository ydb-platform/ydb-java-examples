package tech.ydb.coordination.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.SemaphoreLease;
import tech.ydb.coordination.description.SemaphoreChangedEvent;
import tech.ydb.coordination.description.SemaphoreDescription;
import tech.ydb.coordination.description.SemaphoreWatcher;
import tech.ydb.coordination.settings.DescribeSemaphoreMode;
import tech.ydb.coordination.settings.WatchSemaphoreMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class App {
    private final static Logger logger = LoggerFactory.getLogger(App.class);

    private final static Duration ACQUIRE_TIMEOUT = Duration.ofSeconds(2);

    private final static String PATH = "examples/app";
    private final static String SEMAPHORE_NAME = "app_semaphore";
    private final static long SEMAPHORE_LIMIT = 50;

    private final CoordinationClient client;
    private final ScheduledExecutorService scheduler;

    public App(GrpcTransport transport, ScheduledExecutorService scheduler) {
        this.client = CoordinationClient.newClient(transport);
        this.scheduler = scheduler;
    }

    public void run() {
        createPath();
        try {
            createSemaphore();
            try {
                Runnable stopDescribe = scheduleDescribe();

                logger.info("create workers are finished");
                List<CompletableFuture<Status>> workers = new ArrayList<>();

                // Lock 20 for 10 seconds in 1 second
                workers.add(scheduleWorker(1, 10, 20));
                // Lock 30 for 15 seconds in 2 second
                workers.add(scheduleWorker(2, 10, 30));
                // Lock 10 for 5 seconds in 5 second
                workers.add(scheduleWorker(5, 5, 10));

                logger.info("all workers are finished");
                workers.forEach(CompletableFuture::join);
                logger.info("all workers are finished");

                // stop describe
                stopDescribe.run();
            } finally {
                deleteSemaphore();
            }
        } finally {
            dropPath();
        }
    }

    private void createPath() {
        Status createStatus = client.createNode(PATH).join();
        logger.info("path {} created with status {}", PATH, createStatus);
    }

    private void dropPath() {
        Status createStatus = client.dropNode(PATH).join();
        logger.info("path {} removed with status {}", PATH, createStatus);
    }

    private void createSemaphore() {
        CoordinationSession session = client.createSession(PATH);
        session.connect().join().expectSuccess("cannot start coordination session");
        Status createStatus = session.createSemaphore(SEMAPHORE_NAME, SEMAPHORE_LIMIT).join();
        logger.info("semaphore {} in {} created with status {}", SEMAPHORE_NAME, PATH, createStatus);
        session.stop().join().expectSuccess("cannot stop coordination session");
    }

    private void deleteSemaphore() {
        CoordinationSession session = client.createSession(PATH);
        session.connect().join().expectSuccess("cannot start coordination session");
        Status deleteStatus = session.deleteSemaphore(SEMAPHORE_NAME, false).join();
        logger.info("semaphore {} in {} deleted with status {}", SEMAPHORE_NAME, PATH, deleteStatus);
        session.stop().join().expectSuccess("cannot stop coordination session");
    }

    private void describeSemaphore(AtomicBoolean isStopped) {
        CoordinationSession session = client.createSession(PATH);
        session.connect().join().expectSuccess("cannot start coordination session");
        logger.info("session {} is waiting for semaphore changing", session);
        while (!isStopped.get()) {
            Result<SemaphoreWatcher> result = session.watchSemaphore(SEMAPHORE_NAME,
                    DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS,
                    WatchSemaphoreMode.WATCH_DATA_AND_OWNERS
            ).join();

            if (!result.isSuccess()) {
                logger.warn("got wrong status {} in describe", result.getStatus());
                continue;
            }

            SemaphoreWatcher watcher = result.getValue();

            SemaphoreDescription description = watcher.getDescription();
            logger.info("   semaphore name  -> {}", description.getName());
            logger.info("   semaphore limit -> {}", description.getLimit());
            logger.info("   semaphore count -> {}", description.getCount());

            logger.info("   semaphore owners:");
            for (SemaphoreDescription.Session owner : description.getOwnersList()) {
                logger.info("      session {} with count {}", owner.getId(), owner.getCount());
            }

            Result<SemaphoreChangedEvent> changed = watcher.getChangedFuture().join();
            if (changed.isSuccess()) {
                SemaphoreChangedEvent e = changed.getValue();
                logger.info("got semaphore changed event data={}, owners={}", e.isDataChanged(), e.isOwnersChanged());
            }
        }
        session.stop().join().expectSuccess("cannot stop coordination session");
    }

    private Runnable scheduleDescribe() {
        final AtomicBoolean isStopped = new AtomicBoolean(false);
        scheduler.execute(() -> describeSemaphore(isStopped));
        return () -> isStopped.set(true);
    }

    private SemaphoreLease acquireSemaphore(CoordinationSession session, int count) {
        while (session.getState().isActive()) {
            logger.info("try accept semaphore in session {}", session);
            Result<SemaphoreLease> lease = session.acquireSemaphore(SEMAPHORE_NAME, count, ACQUIRE_TIMEOUT).join();
            logger.info("session {} got acquire semaphore with status {}", session, lease.getStatus());

            if (lease.isSuccess()) {
                return lease.getValue();
            }
        }

        return null;
    }

    private CompletableFuture<Status> scheduleWorker(int delay, int duration, int count) {
        final CompletableFuture<Status> workFuture = new CompletableFuture<>();

        scheduler.schedule(() -> {
            logger.info("create new session for work");
            final CoordinationSession session = client.createSession(PATH);
            session.connect().join().expectSuccess("cannot connect session");

            final SemaphoreLease lease = acquireSemaphore(session, count);

            if (lease != null) {
                scheduler.schedule(() -> {
                    lease.release().join();
                    logger.info("session {} got release semaphore", session);
                    session.stop().join().expectSuccess("cannot stop session");

                    workFuture.complete(Status.SUCCESS);
                }, duration, TimeUnit.SECONDS);
            }
        }, delay, TimeUnit.SECONDS);

        return workFuture;
    }
}
