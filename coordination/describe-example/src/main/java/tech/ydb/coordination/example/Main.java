package tech.ydb.coordination.example;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    private final static Duration ACQUIRE_TIMEOUT = Duration.ofSeconds(2);

    private final static String PATH = "/example/path";
    private final static String SEMAPHORE_NAME = "test_semaphore";
    private final static long SEMAPHORE_LIMIT = 50;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-coordination-describe.jar <connection_url>");
            return;
        }

        try (GrpcTransport transport = GrpcTransport.forConnectionString(args[0]).build()) {
            CoordinationClient client = CoordinationClient.newClient(transport);

            createPath(client);
            createSemaphore(client);

            Runnable stopDescribe = scheduleDescribe(client,  transport.getScheduler());

            logger.info("create workers are finished");
            List<CompletableFuture<Status>> workers = new ArrayList<>();

            // Lock 20 for 10 seconds in 1 second
            workers.add(scheduleWorker(client, transport.getScheduler(), 1, 10, 20));
            // Lock 30 for 15 seconds in 2 second
            workers.add(scheduleWorker(client, transport.getScheduler(), 2, 10, 30));
            // Lock 10 for 5 seconds in 5 second
            workers.add(scheduleWorker(client, transport.getScheduler(), 5, 5, 10));

            logger.info("all workers are finished");
            workers.forEach(CompletableFuture::join);
            logger.info("all workers are finished");

            stopDescribe.run();

            deleteSemaphore(client);
            dropPath(client);
        }
    }

    private static void createPath(CoordinationClient client) {
        String fullPath = client.getDatabase() + PATH;
        Status createStatus = client.createNode(fullPath).join();
        logger.info("path {} created with status {}", fullPath, createStatus);
    }

    private static void dropPath(CoordinationClient client) {
        String fullPath = client.getDatabase() + PATH;
        Status createStatus = client.dropNode(fullPath).join();
        logger.info("path {} removed with status {}", fullPath, createStatus);
    }

    private static void createSemaphore(CoordinationClient client) {
        final String fullPath = client.getDatabase() + PATH;
        try (CoordinationSession session = client.createSession(fullPath)) {
            session.start().join();
            Status createStatus = session.createSemaphore(SEMAPHORE_NAME, SEMAPHORE_LIMIT).join();
            logger.info("semaphore {} in {} created with status {}", SEMAPHORE_NAME, fullPath, createStatus);
        }

//        CoordinationSession session = client.createSession(PATH);
//        CompletableFuture<Status> future = session.start()
//                .thenCompose(id -> session.createSemaphore(SEMAPHORE_NAME, SEMAPHORE_LIMIT))
//                .whenComplete((status, th) -> session.close());
    }

    private static void deleteSemaphore(CoordinationClient client) {
        final String fullPath = client.getDatabase() + PATH;
        try (CoordinationSession session = client.createSession(fullPath)) {
            session.start().join();
            Status deleteStatus = session.deleteSemaphore(SEMAPHORE_NAME, false).join();
            logger.info("semaphore {} in {} deleted with status {}", SEMAPHORE_NAME, fullPath, deleteStatus);
        }
    }

    private static void describeSemaphore(CoordinationClient client, AtomicBoolean isStopped) {
        final String fullPath = client.getDatabase() + PATH;

        try (CoordinationSession session = client.createSession(fullPath)) {
            Long sessionID = session.start().join();
            logger.info("session {} is waiting for semaphore changing", sessionID);
            while (!isStopped.get()) {
                Result<SemaphoreWatcher> result = session.describeAndWatchSemaphore(SEMAPHORE_NAME,
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

                SemaphoreChangedEvent changed = watcher.getChangedFuture().join();
                logger.info("got semaphore shanged event {}", changed);
            }
        }
    }

    private static Runnable scheduleDescribe(CoordinationClient client, ExecutorService executor) {
        final AtomicBoolean isStopped = new AtomicBoolean(false);
        executor.execute(() -> describeSemaphore(client, isStopped));
        return () -> isStopped.set(true);
    }

    private static SemaphoreLease acquireSemaphore(CoordinationSession session, Long sessionID, int count) {
        while (session.getState().isActive()) {
            logger.info("try accept semaphore in session {}", sessionID);
            Result<SemaphoreLease> lease = session.acquireSemaphore(SEMAPHORE_NAME, count, ACQUIRE_TIMEOUT).join();
            logger.info("session {} got acquire semaphore with status {}", sessionID, lease.getStatus());

            if (lease.isSuccess()) {
                return lease.getValue();
            }
        }

        return null;
    }

    private static CompletableFuture<Status> scheduleWorker(
            CoordinationClient client, ScheduledExecutorService scheduler, int delay, int duration, int count) {

        final CompletableFuture<Status> workFuture = new CompletableFuture<>();
        final String fullPath = client.getDatabase() + PATH;

        scheduler.schedule(() -> {
            logger.info("create new session for work");
            final CoordinationSession session = client.createSession(fullPath);
            final Long sessionID = session.start().join();
            final SemaphoreLease lease = acquireSemaphore(session, sessionID, count);

            if (lease != null) {
                scheduler.schedule(() -> {
                    lease.release().join();
                    logger.info("session {} got release semaphore", sessionID);
                    session.close();

                    workFuture.complete(Status.SUCCESS);
                }, duration, TimeUnit.SECONDS);
            }
        }, delay, TimeUnit.SECONDS);

        return workFuture;
    }


    private static void workerExample(CoordinationSession session, int count) {
        while (session.getState().isActive()) {
            Result<SemaphoreLease> result = session.acquireSemaphore(SEMAPHORE_NAME, count, ACQUIRE_TIMEOUT).join();
            if (!result.isSuccess()) {
                // retry
                continue;
            }

            SemaphoreLease lease = result.getValue();
            // make work
            // ...
            lease.release().join();
            return;
        }
    }

    private static CompletableFuture<Void> workerExampleAsync(CoordinationSession session, int count) {
        if (!session.getState().isActive()) {
            // session is closed
            return CompletableFuture.completedFuture(null);
        }

        return session.acquireSemaphore(SEMAPHORE_NAME, count, ACQUIRE_TIMEOUT).thenCompose(result -> {
            if (!result.isSuccess()) {
                // retry
                logger.warn("can't acquire semaphore with status {}", result.getStatus());
                return workerExampleAsync(session, count);
            }

            // make work
            // ...
            return result.getValue().release();
        });
    }

    private static void describeSemaphoreAsync(CoordinationSession session) {
        if (session.getState().isActive()) {
            return;
        }

        session.describeAndWatchSemaphore(
                SEMAPHORE_NAME,
                DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS,
                WatchSemaphoreMode.WATCH_DATA_AND_OWNERS
        ).whenComplete((result, th) -> {
            if (th != null || !result.isSuccess()) {
                // got a problem, retry
                describeSemaphoreAsync(session);
                return;
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

            // rerun after change
            watcher.getChangedFuture().thenRun(() -> describeSemaphoreAsync(session));
        });
    }

}
