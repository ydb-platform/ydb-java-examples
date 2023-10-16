package tech.ydb.coordination.example;




import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.settings.CoordinationNodeSettings;
import tech.ydb.coordination.settings.DropCoordinationNodeSettings;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.proto.coordination.SemaphoreDescription;
import tech.ydb.proto.coordination.SemaphoreSession;
import tech.ydb.proto.coordination.SessionRequest;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    private final static String PATH = "/example/path";
    private final static String SEMAPHORE_NAME = "test_semaphore";
    private final static long SEMAPHORE_LIMIT = 50;
    private final static long SEMAPHORE_TIMEOUT_MS = 2000;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-coordination-describe.jar <connection_url>");
            return;
        }

        try (GrpcTransport transport = GrpcTransport.forConnectionString(args[0]).build()) {
            CoordinationClient client = CoordinationClient.newClient(transport);

            createPath(client);
            createSemaphore(client, transport.getScheduler());

            AtomicBoolean isFinish = new AtomicBoolean(false);
            CoordinationSession describeSession = describeSemaphore(client, isFinish).join();

            List<CompletableFuture<Status>> workers = new ArrayList<>();

            // Lock 20 for 10 seconds in 1 second
            workers.add(scheduleAcquireSemaphore(client, transport.getScheduler(), 1, 10, 20));
            // Lock 30 for 15 seconds in 2 second
            workers.add(scheduleAcquireSemaphore(client, transport.getScheduler(), 2, 10, 30));
            // Lock 10 for 5 seconds in 5 second
            workers.add(scheduleAcquireSemaphore(client, transport.getScheduler(), 5, 5, 10));

            workers.forEach(CompletableFuture::join);

            isFinish.set(true);
            describeSession.stop();

            deleteSemaphore(client, transport.getScheduler());
            dropPath(client);
        }
    }

    private static void createPath(CoordinationClient client) {
        String fullPath = client.getDatabase() + PATH;
        Status createStatus = client.createNode(fullPath, CoordinationNodeSettings.newBuilder().build()).join();
        logger.info("path {} created with status {}", fullPath, createStatus);
    }

    private static void dropPath(CoordinationClient client) {
        String fullPath = client.getDatabase() + PATH;
        Status createStatus = client.dropNode(fullPath, DropCoordinationNodeSettings.newBuilder().build()).join();
        logger.info("path {} removed with status {}", fullPath, createStatus);
    }

    private static void createSemaphore(CoordinationClient client, Executor executor) {
        final CompletableFuture<Status> createResult = new CompletableFuture<>();
        final String fullPath = client.getDatabase() + PATH;
        final CoordinationSession session = client.createSession();

        session.start(new CoordinationSession.Observer() {
            @Override
            public void onSessionStarted() {
                logger.info("session {} started", session.getSessionId());
                // Send create semaphore message
                logger.info("session {} send create semaphore {}", session.getSessionId(), SEMAPHORE_NAME);
                session.sendCreateSemaphore(SessionRequest.CreateSemaphore.newBuilder()
                        .setName(SEMAPHORE_NAME)
                        .setLimit(SEMAPHORE_LIMIT)
                        .build()
                );
            }

            @Override
            public void onCreateSemaphoreResult(Status status) {
                createResult.complete(status);
                // Can't run directrly - dead lock
                executor.execute(session::stop);
            }

            @Override
            public void onFailure(Status status) {
                createResult.complete(status);
                // Can't run directrly - dead lock
                executor.execute(session::stop);
            }
        }).whenComplete((status, th) -> {
            if (th != null) {
                logger.error("session {} finished with exception", session.getSessionId(), th);
                createResult.completeExceptionally(th);
            }
            if (status != null) {
                logger.info("session {} finished with status {}", session.getSessionId(), status);
                createResult.complete(status);
            }
        });

        // Send start session
        session.sendStartSession(SessionRequest.SessionStart.newBuilder()
                .setPath(fullPath)
                .build()
        );

        Status createStatus = createResult.join();
        logger.info("semaphore {} in {} created with status {}", SEMAPHORE_NAME, fullPath, createStatus);
    }

    private static void deleteSemaphore(CoordinationClient client, Executor executor) {
        final CompletableFuture<Status> deleteResult = new CompletableFuture<>();
        final String fullPath = client.getDatabase() + PATH;
        final CoordinationSession session = client.createSession();

        session.start(new CoordinationSession.Observer() {
            @Override
            public void onSessionStarted() {
                logger.info("session {} started", session.getSessionId());
                // Send delete semaphore message
                logger.info("session {} send delete semaphore {}", session.getSessionId(), SEMAPHORE_NAME);
                session.sendDeleteSemaphore(SessionRequest.DeleteSemaphore.newBuilder()
                        .setName(SEMAPHORE_NAME)
                        .build()
                );
            }

            @Override
            public void onDeleteSemaphoreResult(Status status) {
                deleteResult.complete(status);
                // Can't run directrly - dead lock
                executor.execute(session::stop);
            }

            @Override
            public void onFailure(Status status) {
                deleteResult.complete(status);
                // Can't run directrly - dead lock
                executor.execute(session::stop);
            }
        }).whenComplete((status, th) -> {
            if (th != null) {
                logger.error("session {} finished with exception", session.getSessionId(), th);
                deleteResult.completeExceptionally(th);
            }
            if (status != null) {
                logger.info("session {} finished with status {}", session.getSessionId(), status);
                deleteResult.complete(status);
            }
        });

        // Send start session
        session.sendStartSession(SessionRequest.SessionStart.newBuilder()
                .setPath(fullPath)
                .build()
        );

        Status createStatus = deleteResult.join();
        logger.info("semaphore {} in {} deleted with status {}", SEMAPHORE_NAME, fullPath, createStatus);
    }

    private static CompletableFuture<CoordinationSession> describeSemaphore(
            CoordinationClient client, AtomicBoolean isStop) {
        final CompletableFuture<CoordinationSession> sessionFuture = new CompletableFuture<>();

        final String fullPath = client.getDatabase() + PATH;
        final CoordinationSession session = client.createSession();

        session.start(new CoordinationSession.Observer() {
            @Override
            public void onSessionStarted() {
                logger.info("session {} started", session.getSessionId());
                // Send describe semaphore message
                logger.info("session {} send describe semaphore {}", session.getSessionId(), SEMAPHORE_NAME);
                session.sendDescribeSemaphore(SessionRequest.DescribeSemaphore.newBuilder()
                        .setName(SEMAPHORE_NAME)
                        .setIncludeOwners(true)
                        .setIncludeWaiters(true)
                        .setWatchData(true)
                        .setWatchOwners(true)
                        .build()
                );
            }

            @Override
            public void onDescribeSemaphoreResult(SemaphoreDescription description, Status status) {
                sessionFuture.complete(session);

                logger.info("session {} got describe semaphore result with status {}", session.getSessionId(), status);
                if (status.isSuccess()) {
                    logger.info("   semaphore name  -> {}", description.getName());
                    logger.info("   semaphore limit -> {}", description.getLimit());
                    logger.info("   semaphore count -> {}", description.getCount());

                    logger.info("   semaphore owners:");
                    for (SemaphoreSession owner: description.getOwnersList()) {
                        logger.info("      session {} with count {}", owner.getSessionId(), owner.getCount());
                    }
                }
            }

            @Override
            public void onDescribeSemaphoreChanged(boolean dataChanged, boolean ownersChanged) {
                logger.info("session {} got describe semaphore changed with data {} and owners {}",
                        session.getSessionId(), dataChanged, ownersChanged);

                if (isStop.get()) {
                    return;
                }

                logger.info("session {} resend describe semaphore {}", session.getSessionId(), SEMAPHORE_NAME);
                session.sendDescribeSemaphore(SessionRequest.DescribeSemaphore.newBuilder()
                        .setName(SEMAPHORE_NAME)
                        .setIncludeOwners(true)
                        .setIncludeWaiters(true)
                        .setWatchData(true)
                        .setWatchOwners(true)
                        .build()
                );
            }
        }).whenComplete((status, th) -> {
            sessionFuture.complete(session);
            if (th != null) {
                logger.error("session {} finished with exception", session.getSessionId(), th);
            }
            if (status != null) {
                logger.info("session {} finished with status {}", session.getSessionId(), status);
            }
        });

        // Send start session
        session.sendStartSession(SessionRequest.SessionStart.newBuilder()
                .setPath(fullPath)
                .build()
        );

        return sessionFuture;
    }

    private static CompletableFuture<Status> scheduleAcquireSemaphore(
            CoordinationClient client, ScheduledExecutorService scheduler, int delay, int duration, int count) {

        final CompletableFuture<Status> workFuture = new CompletableFuture<>();
        final String fullPath = client.getDatabase() + PATH;
        final CoordinationSession session = client.createSession();

        session.start(new CoordinationSession.Observer() {
            @Override
            public void onSessionStarted() {
                logger.info("session {} started", session.getSessionId());
                // Send acquire semaphore message
                logger.info("session {} send acquire semaphore {} with count {}",
                        session.getSessionId(), SEMAPHORE_NAME, count);
                session.sendAcquireSemaphore(SessionRequest.AcquireSemaphore.newBuilder()
                        .setName(SEMAPHORE_NAME)
                        .setCount(count)
                        .setTimeoutMillis(SEMAPHORE_TIMEOUT_MS)
                        .build()
                );
            }

            @Override
            public void onAcquireSemaphoreResult(boolean acquired, Status status) {
                logger.info("session {} got acquire semaphore result {}  with status {}",
                        session.getSessionId(), acquired, status);

                if (!status.isSuccess()) {
                    workFuture.complete(status);
                    // Can't run directrly - dead lock
                    scheduler.execute(session::stop);
                    return;
                }

                if (!acquired) {
                    // Retry acquire
                    logger.info("session {} resend acquire semaphore {} with count {}",
                            session.getSessionId(), SEMAPHORE_NAME, count);
                    session.sendAcquireSemaphore(SessionRequest.AcquireSemaphore.newBuilder()
                            .setName(SEMAPHORE_NAME)
                            .setCount(count)
                            .setTimeoutMillis(SEMAPHORE_TIMEOUT_MS)
                            .build()
                    );
                    return;
                }

                scheduler.schedule(() -> {
                    // Send release semaphore message
                    logger.info("session {} send release semaphore {}", session.getSessionId(), SEMAPHORE_NAME);
                    session.sendReleaseSemaphore(SessionRequest.ReleaseSemaphore.newBuilder()
                            .setName(SEMAPHORE_NAME)
                            .build()
                    );
                }, duration, TimeUnit.SECONDS);
            }

            @Override
            public void onReleaseSemaphoreResult(boolean released, Status status) {
                logger.info("session {} got release semaphore result {} with status {}",
                        session.getSessionId(), released, status);

                workFuture.complete(status);
                // Can't run directrly - dead lock
                scheduler.execute(session::stop);
            }
        }).whenComplete((status, th) -> {
            if (th != null) {
                logger.error("session {} finished with exception", session.getSessionId(), th);
            }
            if (status != null) {
                logger.info("session {} finished with status {}", session.getSessionId(), status);
            }
        });

        // Send start session
        session.sendStartSession(SessionRequest.SessionStart.newBuilder()
                .setPath(fullPath)
                .build()
        );

        return workFuture;
    }
}
