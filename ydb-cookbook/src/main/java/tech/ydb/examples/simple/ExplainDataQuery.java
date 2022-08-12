package tech.ydb.examples.simple;

import java.time.Duration;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.ExplainDataQueryResult;


/**
 * @author Sergey Polovko
 */
public class ExplainDataQuery extends SimpleExample {

    @Override
    void run(GrpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();
        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession(Duration.ofSeconds(5)).join().getValue()
                ) {

            session.dropTable(tablePath)
                .join();

            String query1 =
                "CREATE TABLE [" + tablePath + "] (" +
                    "  key Uint32," +
                    "  value String," +
                    "  PRIMARY KEY(key)" +
                    ");";
            session.executeSchemeQuery(query1)
                .join()
                .expectSuccess("cannot create table");


            String query2 = "SELECT * FROM [" + tablePath + "];";
            ExplainDataQueryResult result = session.explainDataQuery(query2)
                .join()
                .getValue();

            System.out.println("--[ast]----------------------\n" + result.getQueryAst());
            System.out.println();
            System.out.println("--[plan]---------------------\n" + result.getQueryPlan());
        }
    }

    public static void main(String[] args) {
        new ExplainDataQuery().doMain();
    }
}
