package tech.ydb.demo.ydb;

import java.time.Duration;
import tech.ydb.core.grpc.GrpcTransport;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 *
 * @author Alexandr Gorshenin
 */
public class YdbDriver implements AutoCloseable {
    private final TableClient tableClient;
    private final String database;
    private final SessionRetryContext retryContext;

    public YdbDriver(GrpcTransport transport, String database) throws Exception {
        this.tableClient = TableClient.newClient(transport).build();

        this.retryContext = SessionRetryContext.create(tableClient)
                .maxRetries(5)
                .sessionSupplyTimeout(Duration.ofSeconds(3))
                .build();

        this.database = database;
    }

    public String database() {
        return this.database;
    }

    public SessionRetryContext retryCtx() {
        return this.retryContext;
    }

    @Override
    public void close() {
        this.tableClient.close();
    }
}
