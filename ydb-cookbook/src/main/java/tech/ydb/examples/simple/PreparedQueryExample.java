package tech.ydb.examples.simple;

import java.time.Duration;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.DropTableSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;


/**
 * @author Sergey Polovko
 */
public class PreparedQueryExample extends SimpleExample {

    @Override
    void run(GrpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();

        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession(Duration.ofSeconds(5)).join().getValue()
                ) {
            session.dropTable(tablePath, new DropTableSettings())
                    .join();
            
            TableDescription tableDescription = TableDescription.newBuilder()
                    .addNullableColumn("name", PrimitiveType.Text)
                    .addNullableColumn("size", PrimitiveType.Uint64)
                    .setPrimaryKey("name")
                    .build();
            
            session.createTable(tablePath, tableDescription)
                    .join()
                    .expectSuccess("cannot create table");
            
            DataQuery query1 = session.prepareDataQuery(
                    "DECLARE $name AS Utf8;" +
                            "DECLARE $size AS Uint32;" +
                            "INSERT INTO [" + tablePath + "] (name, size) VALUES ($name, $size);")
                    .join()
                    .getValue();
            
            Params params = query1.newParams()
                    .put("$name", PrimitiveValue.newText("/etc/passwd"))
                    .put("$size", PrimitiveValue.newUint32(42));
            
            DataQueryResult result1 = query1.execute(TxControl.serializableRw().setCommitTx(true), params)
                    .join()
                    .getValue();
            
            DataQueryResults.print(result1);
            
            String query2 = "SELECT * FROM [" + tablePath + "];";
            
            DataQueryResult result2 = session.executeDataQuery(query2, TxControl.serializableRw().setCommitTx(true))
                    .join()
                    .getValue();
            
            DataQueryResults.print(result2);
        }
    }

    public static void main(String[] args) {
        new PreparedQueryExample().doMain();
    }
}
