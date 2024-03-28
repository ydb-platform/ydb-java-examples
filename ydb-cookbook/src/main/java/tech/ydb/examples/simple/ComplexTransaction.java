package tech.ydb.examples.simple;

import java.time.Duration;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;


/**
 * @author Sergey Polovko
 */
public class ComplexTransaction extends SimpleExample {

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();

        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession(Duration.ofSeconds(5)).join().getValue()
                ) {

            session.dropTable(tablePath)
                .join();

            TableDescription tableDescription = TableDescription.newBuilder()
                .addNullableColumn("key", PrimitiveType.Uint32)
                .addNullableColumn("value", PrimitiveType.Text)
                .setPrimaryKey("key")
                .build();

            session.createTable(tablePath, tableDescription)
                .join()
                .expectSuccess("cannot create table");

            TableTransaction transaction = session.beginTransaction(TxMode.SERIALIZABLE_RW)
                .join().getValue();

            String query1 = "UPSERT INTO `" + tablePath + "` (key, value) VALUES (1, 'one');";
            DataQueryResult result1 = transaction.executeDataQuery(query1).join().getValue();
            System.out.println("--[insert1]-------------------");
            DataQueryResults.print(result1);
            System.out.println("------------------------------");

            String query2 = "UPSERT INTO `" + tablePath + "` (key, value) VALUES (2, 'two');";
            DataQueryResult result2 = transaction.executeDataQuery(query2).join().getValue();
            System.out.println("--[insert2]-------------------");
            DataQueryResults.print(result2);
            System.out.println("------------------------------");

            String query3 = "SELECT * FROM `" + tablePath + "`;";
            DataQueryResult result3 = session.executeDataQuery(query3, TxControl.onlineRo().setCommitTx(true))
                .join()
                .getValue();
            System.out.println("--[before commit]-------------");
            DataQueryResults.print(result3);
            System.out.println("------------------------------");

            transaction.commit()
                .join()
                .expectSuccess("cannot commit transaction");

            String query = "SELECT * FROM `" + tablePath + "`;";
            DataQueryResult result = session.executeDataQuery(query, TxControl.onlineRo().setCommitTx(true))
                .join()
                .getValue();
            System.out.println("--[after commit]-------------");
            DataQueryResults.print(result);
            System.out.println("------------------------------");
        }
    }

    public static void main(String[] args) {
        new ComplexTransaction().doMain(args);
    }

    public static int test(String[] args) {
        new ComplexTransaction().doMain(args);
        return 0;
    }
}
