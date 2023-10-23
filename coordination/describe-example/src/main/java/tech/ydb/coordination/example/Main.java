package tech.ydb.coordination.example;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSessionNew;
import tech.ydb.coordination.settings.CoordinationNodeSettings;
import tech.ydb.coordination.settings.DropCoordinationNodeSettings;
import tech.ydb.coordination.settings.SemaphoreDescription;
import tech.ydb.coordination.settings.SemaphoreSession;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    private final static Duration CREATE_TIMEOUT = Duration.ofSeconds(5);
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

            try (CoordinationSessionNew session = describeSemaphore(client, transport.getScheduler())) {
                logger.info("session {} is waiting for semaphore changing", session.getId());

                List<CompletableFuture<Status>> workers = new ArrayList<>();

//                workers.add(new CompletableFuture<>());
//                transport.getScheduler().schedule(() -> workers.get(0).complete(Status.SUCCESS), 10, TimeUnit.SECONDS);

                // Lock 20 for 10 seconds in 1 second
                workers.add(scheduleAcquireSemaphore(client, transport.getScheduler(), 1, 10, 20));
//                // Lock 30 for 15 seconds in 2 second
//                workers.add(scheduleAcquireSemaphore(client, transport.getScheduler(), 2, 10, 30));
//                // Lock 10 for 5 seconds in 5 second
//                workers.add(scheduleAcquireSemaphore(client, transport.getScheduler(), 5, 5, 10));
//
                workers.forEach(CompletableFuture::join);
            }

            deleteSemaphore(client);
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

    private static void createSemaphore(CoordinationClient client) {
        final String fullPath = client.getDatabase() + PATH;
        try (CoordinationSessionNew session = client.createSession(fullPath, CREATE_TIMEOUT).join()) {
            Status createStatus = session.createSemaphore(SEMAPHORE_NAME, SEMAPHORE_LIMIT).join();
            logger.info("semaphore {} in {} created with status {}", SEMAPHORE_NAME, fullPath, createStatus);
        }
    }

    private static void deleteSemaphore(CoordinationClient client) {
        final String fullPath = client.getDatabase() + PATH;
        try (CoordinationSessionNew session = client.createSession(fullPath, CREATE_TIMEOUT).join()) {
            Status createStatus = session.deleteSemaphore(SEMAPHORE_NAME, false).join();
            logger.info("semaphore {} in {} deleted with status {}", SEMAPHORE_NAME, fullPath, createStatus);
        }
    }

    private static CoordinationSessionNew describeSemaphore(CoordinationClient client, ExecutorService service) {
        final String fullPath = client.getDatabase() + PATH;
        CoordinationSessionNew session = client.createSession(fullPath, CREATE_TIMEOUT).join();

        final Runnable[] describeRunnable = new Runnable[1];
        describeRunnable[0] = () -> {
            Result<SemaphoreDescription> result = session.describeSemaphore(SEMAPHORE_NAME,
                    CoordinationSessionNew.DescribeMode.WITH_OWNERS_AND_WAITERS,
                    CoordinationSessionNew.WatchMode.WATCH_DATA_AND_OWNERS, (changed) -> {
                        logger.info("session {} got describe semaphore changed with data {} and owners {}",
                                session.getId(), changed.isDataChanged(), changed.isOwnersChanged());
                        //
                        service.execute(describeRunnable[0]);
                    }
            ).join();

            logger.info("session {} got describe semaphore result with status {}", session.getId(), result.getStatus());
            if (result.isSuccess()) {
                SemaphoreDescription description = result.getValue();
                logger.info("   semaphore name  -> {}", description.getName());
                logger.info("   semaphore limit -> {}", description.getLimit());
                logger.info("   semaphore count -> {}", description.getCount());

                logger.info("   semaphore owners:");
                for (SemaphoreSession owner : description.getOwnersList()) {
                    logger.info("      session {} with count {}", owner.getId(), owner.getCount());
                }
            }
        };

        service.execute(describeRunnable[0]);
        return session;
    }

    private static CoordinationSessionNew.CoordinationSemaphore acquire(CoordinationSessionNew session, int count) {
        while (true) {
            Result<CoordinationSessionNew.CoordinationSemaphore> result = session
                    .acquireSemaphore(SEMAPHORE_NAME, count, ACQUIRE_TIMEOUT).join();

            logger.info("session {} got acquire semaphore with status {}",
                    session.getId(), result.getStatus());

            if (result.isSuccess()) {
                return result.getValue();
            }
        }
    }

    private static Boolean release(CoordinationSessionNew session, CoordinationSessionNew.CoordinationSemaphore semaphore) {
        while (true) {
            Result<Boolean> result = semaphore.release().join();

            logger.info("session {} got release semaphore with status {}",
                    session.getId(), result.getStatus());

            if (result.isSuccess()) {
                return result.getValue();
            }
        }
    }

    private static CompletableFuture<Status> scheduleAcquireSemaphore(
            CoordinationClient client, ScheduledExecutorService scheduler, int delay, int duration, int count) {

        final CompletableFuture<Status> workFuture = new CompletableFuture<>();
        final String fullPath = client.getDatabase() + PATH;

        scheduler.schedule(() -> {
            logger.info("create new session for work");
            client.createSession(fullPath, CREATE_TIMEOUT).whenComplete((session, th) -> {
                if (th != null || session == null) {
                    logger.error("create session problem", th);
                    workFuture.completeExceptionally(th);
                    return;
                }

                logger.info("try accept semaphore");
                final CoordinationSessionNew.CoordinationSemaphore semaphore = acquire(session, count);
                logger.info("accepted semaphore");
                scheduler.schedule(() -> {
                    semaphore.release().whenComplete((status, th2) -> {
                        if (th2 != null || status == null) {
                            workFuture.completeExceptionally(th2);
                            session.close();
                            return;
                        }

                        release(session, semaphore);
                        session.close();

                        workFuture.complete(Status.SUCCESS);
                    });
                }, duration, TimeUnit.SECONDS);
            });

        }, delay, TimeUnit.SECONDS);

        return workFuture;
    }
}
