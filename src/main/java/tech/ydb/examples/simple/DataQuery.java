package tech.ydb.examples.simple;

import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.TableService;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.settings.AutoPartitioningPolicy;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.PartitioningPolicy;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.types.PrimitiveType;


/**
 * @author Sergey Polovko
 */
public class DataQuery extends SimpleExample {

    @Override
    void run(TableService tableService, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();
        TableClient tableClient = tableService.newTableClient();

        Session session = tableClient.createSession()
            .join()
            .expect("cannot create session");

        session.dropTable(tablePath)
            .join();

        {
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
        }

        {
            String query = "INSERT INTO [" + tablePath + "] (id, login, age) VALUES (1, 'Jamel', 99);";
            DataQueryResult result = session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
                .join()
                .expect("query failed");
            DataQueryResults.print(result);
        }

        {
            String query = "SELECT * FROM [" + tablePath + "];";
            DataQueryResult result = session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
                .join()
                .expect("query failed");
            DataQueryResults.print(result);
        }

        session.close()
            .join()
            .expect("cannot close session");
    }

    public static void main(String[] args) {
        new DataQuery().doMain();
    }
}
