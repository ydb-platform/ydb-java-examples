package tech.ydb.examples;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.PrimitiveType;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

/**
 *
 * @author Alexandr Gorshenin
 */
public class YdbDockerContainer extends GenericContainer<YdbDockerContainer> {
    private static final Logger log = LoggerFactory.getLogger(YdbDockerContainer.class);

    private static final String DEFAULT_YDB_IMAGE = "cr.yandex/yc/yandex-docker-local-ydb:latest";
    private static final String DOCKER_DATABASE = "/local";
    private static final String PEM_PATH = "/ydb_certs/ca.pem";

    private final int grpcsPort; // Secure connection
    private final int grpcPort;  // Non secure connection

    YdbDockerContainer(String image) {
        super(image);

        PortsGenerator gen = new PortsGenerator();
        grpcsPort = gen.findAvailablePort();
        grpcPort = gen.findAvailablePort();

        addExposedPort(grpcPort); // don't expose by default

        // Host ports and container ports MUST BE equal - ydb implementation limitation
        addFixedExposedPort(grpcsPort, grpcsPort);
        addFixedExposedPort(grpcPort, grpcPort);

        withEnv("GRPC_PORT", String.valueOf(grpcPort));
        withEnv("GRPC_TLS_PORT", String.valueOf(grpcsPort));

        withCreateContainerCmdModifier(modifier -> modifier
                .withName("ydb-" + UUID.randomUUID())
                .withHostName(getHost()));
        waitingFor(new YdbCanCreateTableWaitStrategy());
    }

    public String nonSecureEndpoint() {
        return String.format("%s:%s", getHost(), grpcPort);
    }

    public String secureEndpoint() {
        return String.format("%s:%s", getHost(), grpcsPort);
    }

    public byte[] pemCert() {
        return copyFileFromContainer(PEM_PATH, is -> {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(is, baos);
            return baos.toByteArray();
        });
    }

    public String database() {
        return DOCKER_DATABASE;
    }

    public static YdbDockerContainer createAndStart() {
        String customImage = System.getProperty("YDB_IMAGE", DEFAULT_YDB_IMAGE);
        YdbDockerContainer container = new YdbDockerContainer(customImage);
        container.start();

        return container;
    }


    private class YdbCanCreateTableWaitStrategy extends AbstractWaitStrategy {
        private static final String DOCKER_INIT_TABLE = DOCKER_DATABASE + "/docker_init_table";

        @Override
        protected void waitUntilReady() {
            // Wait 30 second for start of ydb
            Unreliables.retryUntilSuccess(30, TimeUnit.SECONDS, () -> {
                getRateLimiter().doWhenReady(() -> {
                    GrpcTransportBuilder transportBuilder = GrpcTransport.forEndpoint(nonSecureEndpoint(), database());
                    try (GrpcTransport transport = transportBuilder.build()) {
                        try (TableClient tableClient = TableClient.newClient(transport).build()) {

                            Session session = tableClient.createSession(Duration.ofSeconds(5))
                                    .get().getValue();

                            Status status = session.createTable(
                                    DOCKER_INIT_TABLE,
                                    TableDescription
                                            .newBuilder()
                                            .addNullableColumn("id", PrimitiveType.Text)
                                            .setPrimaryKey("id")
                                            .build()
                            ).get();
                            if (!status.isSuccess()) {
                                throw new UnexpectedResultException("can't create table", status);
                            }
                        }
                    } catch (InterruptedException e) {
                        log.warn("execution interrupted {}", e.getMessage());
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("interrupted", e);
                    } catch (Exception e) {
                        log.info("execution problem {}", e.getMessage());
                        throw new RuntimeException("don't ready", e);
                    }
                });
                log.info("Done");
                return true;
            });
        }
    }
}