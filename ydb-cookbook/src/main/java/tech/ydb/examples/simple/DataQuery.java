package tech.ydb.examples.simple;

import tech.ydb.core.grpc.GrpcTransport;
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
    void run(GrpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();
        TableClient tableClient = TableClient.newClient(transport).build();

        Session session = tableClient.createSession()
            .join()
            .expect("cannot create session");

        session.dropTable(tablePath)
            .join();

        TableDescription tableDescription = TableDescription.newBuilder()
            .addNullableColumn("id", PrimitiveType.uint32())
            .addNullableColumn("login", PrimitiveType.string())
            .addNullableColumn("age", PrimitiveType.uint32())
            .setPrimaryKey("id")
            .build();

        CreateTableSettings settings = new CreateTableSettings()
            .setPartitioningPolicy(new PartitioningPolicy()
                .setAutoPartitioning(AutoPartitioningPolicy.AUTO_SPLIT)
                .setUniformPartitions(4));

        session.createTable(tablePath, tableDescription, settings)
            .join()
            .expect("cannot create table");


        String query1 = "INSERT INTO [" + tablePath + "] (id, login, age) VALUES (1, 'Jamel', 99);";
        DataQueryResult result1 = session.executeDataQuery(query1, TxControl.serializableRw().setCommitTx(true))
            .join()
            .expect("query failed");
        DataQueryResults.print(result1);


        String query2 = "SELECT * FROM [" + tablePath + "];";
        DataQueryResult result2 = session.executeDataQuery(query2, TxControl.serializableRw().setCommitTx(true))
            .join()
            .expect("query failed");
        DataQueryResults.print(result2);

        session.close()
            .join()
            .expect("cannot close session");
    }

    public static void main(String[] args) {
        new DataQuery().doMain();
    }
}
