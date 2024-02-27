package tech.ydb.coordination.example;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.settings.CoordinationSessionSettings;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class AsyncApp {
    private final static Logger logger = LoggerFactory.getLogger(AsyncApp.class);

    private final static Duration ACQUIRE_TIMEOUT = Duration.ofSeconds(2);

    private final static String PATH = "examples/async_app";
    private final static String SEMAPHORE_NAME = "app_semaphore";
    private final static long SEMAPHORE_LIMIT = 50;

    private final CoordinationClient client;
    private final ScheduledExecutorService scheduler;

    public AsyncApp(GrpcTransport transport, ScheduledExecutorService scheduler) {
        this.client = CoordinationClient.newClient(transport);
        this.scheduler = scheduler;
    }

    public CompletableFuture<Status> run() {
        return createPath().thenCompose(Status.compose(() -> {
            CompletableFuture<Status> future = createSemaphore().thenCompose(Status.compose(() -> {
                CompletableFuture<Status> work = CompletableFuture.completedFuture(Status.SUCCESS);
                return deleteSemaphoreAfterAndCombine(work);
            }));

            return dropPathAfterAndCombine(future);
        }));
//        Runnable stopDescribe = scheduleDescribe();
//
//        logger.info("create workers are finished");
//        List<CompletableFuture<Status>> workers = new ArrayList<>();
//
//        // Lock 20 for 10 seconds in 1 second
//        workers.add(scheduleWorker(1, 10, 20));
//        // Lock 30 for 15 seconds in 2 second
//        workers.add(scheduleWorker(2, 10, 30));
//        // Lock 10 for 5 seconds in 5 second
//        workers.add(scheduleWorker(5, 5, 10));
//
//        logger.info("all workers are finished");
//        workers.forEach(CompletableFuture::join);
//        logger.info("all workers are finished");
//
//        // stop describe
//        stopDescribe.run();
//
//        deleteSemaphore();
//        dropPath();
    }

    private CompletableFuture<Status> createPath() {
        return client.createNode(PATH).thenApply(status -> {
            logger.info("path {} created with status {}", PATH, status);
            return status;
        });
    }

    private CompletableFuture<Status> dropPathAfterAndCombine(CompletableFuture<Status> future) {
        // always drop node after comleting of future
        final CompletableFuture<Status> drop = future.handle((s, t) -> s)
                .thenCompose(s -> client.dropNode(PATH))
                .thenApply(status -> {
                    logger.info("path {} dropped with status {}", PATH, status);
                    return status;
                });

        // and combine results
        return future.thenCombine(drop, (fStatus, dStatus) -> fStatus.isSuccess() ? dStatus : fStatus);
    }

    private CompletableFuture<Result<CoordinationSession>> createSession() {
        final CoordinationSession session = client.createSession(PATH, CoordinationSessionSettings.newBuilder()
                .withExecutor(r -> r.run())
                .build()
        );

        return session.connect().thenApply(status -> {
            logger.info("session {} started with status {}", session, status);
            return status;
        }).thenCompose(Status.bindValue(session));
    }

    private CompletableFuture<Status> stopSessionAfterAndCombine(
            CoordinationSession session, CompletableFuture<Status> future
    ) {
        // always stop session after comleting of future
        final CompletableFuture<Status> stop = future.handle((s, t) -> s)
                .thenCompose(s -> session.stop())
                .thenApply(status -> {
                    logger.info("session {} stopped with status {}", session, status);
                    return status;
                });

        // and combine results
        return future.thenCombine(stop, (fStatus, sStatus) -> fStatus.isSuccess() ? sStatus : fStatus);
    }

    private CompletableFuture<Status> createSemaphore() {
        return createSession().thenCompose(Result.composeStatus(session -> {
            CompletableFuture<Status> future = session.createSemaphore(PATH, SEMAPHORE_LIMIT).thenApply(status -> {
                logger.info("semaphore {} created with status {}", PATH, status);
                return status;
            });
            return stopSessionAfterAndCombine(session, future);
        }));
    }

    private CompletableFuture<Status> deleteSemaphoreAfterAndCombine(CompletableFuture<Status> future) {
        // always drop semaphore after comleting of future
        final CompletableFuture<Status> delete = future
                .handle((s, t) -> s)
                .thenCompose(s -> createSession().thenCompose(Result.composeStatus(session -> {
                        CompletableFuture<Status> d = session.deleteSemaphore(PATH).thenApply(status -> {
                            logger.info("semaphore {} deleted with status {}", PATH, status);
                            return status;
                        });
                        return stopSessionAfterAndCombine(session, d);
                })));

        // and combine results
        return future.thenCombine(delete, (fStatus, dStatus) -> fStatus.isSuccess() ? dStatus : fStatus);
    }

//    private static CompletableFuture<Void> workerExampleAsync(CoordinationSession session, int count) {
//        if (!session.getState().isActive()) {
//            // session is closed
//            return CompletableFuture.completedFuture(null);
//        }
//
//        return session.acquireSemaphore(SEMAPHORE_NAME, count, ACQUIRE_TIMEOUT).thenCompose(result -> {
//            if (!result.isSuccess()) {
//                // retry
//                logger.warn("can't acquire semaphore with status {}", result.getStatus());
//                return workerExampleAsync(session, count);
//            }
//
//            // make work
//            // ...
//            return result.getValue().release();
//        });
//    }
//
//    private CompletableFuture<Status> describeSemaphore(CoordinationSession session) {
//        return session.watchSemaphore(
//                SEMAPHORE_NAME,
//                DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS,
//                WatchSemaphoreMode.WATCH_DATA_AND_OWNERS
//        ).handle((result, th) -> {
//            if (!session.getState().isConnected()) {
//                return Status.
//            }
//            if (th != null || !result.isSuccess()) {
//                // got a problem, retry
//                describeSemaphoreAsync(session);
//                return;
//            }
//
//            SemaphoreWatcher watcher = result.getValue();
//
//            SemaphoreDescription description = watcher.getDescription();
//            logger.info("   semaphore name  -> {}", description.getName());
//            logger.info("   semaphore limit -> {}", description.getLimit());
//            logger.info("   semaphore count -> {}", description.getCount());
//
//            logger.info("   semaphore owners:");
//            for (SemaphoreDescription.Session owner : description.getOwnersList()) {
//                logger.info("      session {} with count {}", owner.getId(), owner.getCount());
//            }
//
//            // rerun after change
//            watcher.getChangedFuture().thenRun(() -> describeSemaphoreAsync(session));
//        });
//    }
}
