package tech.ydb.examples.simple;

import java.time.Duration;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.settings.AutoPartitioningPolicy;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.PartitioningPolicy;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;


/**
 * @author Sergey Polovko
 */
public class DataQuery extends SimpleExample {

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();
        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession(Duration.ofSeconds(5)).join().getValue();
                ) {

            session.dropTable(tablePath)
                .join();

            TableDescription tableDescription = TableDescription.newBuilder()
                .addNullableColumn("id", PrimitiveType.Uint32)
                .addNullableColumn("login", PrimitiveType.Text)
                .addNullableColumn("age", PrimitiveType.Uint32)
                .setPrimaryKey("id")
                .build();

            CreateTableSettings settings = new CreateTableSettings()
                .setPartitioningPolicy(new PartitioningPolicy()
                    .setAutoPartitioning(AutoPartitioningPolicy.AUTO_SPLIT)
                    .setUniformPartitions(4));

            session.createTable(tablePath, tableDescription, settings)
                .join()
                .expectSuccess("cannot create table");


            String query1 = "INSERT INTO [" + tablePath + "] (id, login, age) VALUES (1, 'Jamel', 99);";
            DataQueryResult result1 = session.executeDataQuery(query1, TxControl.serializableRw().setCommitTx(true))
                .join().getValue();
            DataQueryResults.print(result1);


            String query2 = "SELECT * FROM [" + tablePath + "];";
            DataQueryResult result2 = session.executeDataQuery(query2, TxControl.serializableRw().setCommitTx(true))
                .join().getValue();
            DataQueryResults.print(result2);
        }
    }

    public static void main(String[] args) {
        new DataQuery().doMain(args);
    }
}
