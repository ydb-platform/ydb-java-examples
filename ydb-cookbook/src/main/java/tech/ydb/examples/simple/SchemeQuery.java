package tech.ydb.examples.simple;

import java.time.Duration;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;


/**
 * @author Sergey Polovko
 */
public class SchemeQuery extends SimpleExample {

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();

        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession(Duration.ofSeconds(5)).join().getValue()
                ) {
            String createTable =
                    "CREATE TABLE [" + tablePath + "] (" +
                    "  key Uint32," +
                    "  value String," +
                    "  PRIMARY KEY(key)" +
                    ");";
            
            session.executeSchemeQuery(createTable)
                    .join()
                    .expectSuccess("cannot create table");
            
            session.executeSchemeQuery("DROP TABLE [" + tablePath + "];")
                    .join()
                    .expectSuccess("cannot drop table");
        }
    }

    public static void main(String[] args) {
        new SchemeQuery().doMain(args);
    }
}
