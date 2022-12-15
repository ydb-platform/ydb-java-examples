package tech.ydb.examples.simple;

import java.time.Duration;
import java.util.UUID;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.DropTableSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;


public class BasicWorkflow extends SimpleExample {

    private void makeDirectory(SchemeClient schemeClient, String directoryPath) {
        schemeClient.makeDirectory(directoryPath)
                .join()
                .expectSuccess("cannot make directory: " + directoryPath);
    }

    private void removeDir(SchemeClient schemeClient, String directoryPath) {
        schemeClient.removeDirectory(directoryPath)
                .join()
                .expectSuccess("cannot remove directory: " + directoryPath);
    }

    private Session makeSession(TableClient tableClient) {
        return tableClient.createSession(Duration.ofSeconds(5))
            .join()
            .getValue();
    }

    private void createOrdersTable(Session session, String tablePath) {
        TableDescription tableDescription = TableDescription.newBuilder()
            .addNullableColumn("id", PrimitiveType.Uint32)
            .addNullableColumn("counterparty", PrimitiveType.Bytes)
            .addNullableColumn("security", PrimitiveType.Bytes)
            .addNullableColumn("amount", PrimitiveType.Uint32)
            .setPrimaryKey("id")
            .build();

        session.createTable(tablePath, tableDescription)
                .join()
                .expectSuccess("cannot create table");
    }

    private void dropTable(Session session, String tablePath) {
        session.dropTable(tablePath, new DropTableSettings())
                .join();

    }

    DataQueryResult executeQuery(Session session, String query) {
        Result<DataQueryResult> result = session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
            .join();
        System.out.println("Status: " + result.getStatus());
        return result.getValue();
    }

    DataQuery makePreparedQuery(Session session, String query) {
        return session.prepareDataQuery(query)
            .join()
            .getValue();
    }

    void processBasicData(Session session, String tablePath) {
        String query1 = "INSERT INTO [" + tablePath + "] (id, counterparty, security, amount) " +
                "VALUES (1, 'Fedex', 'tmob', 5000), (2, 'Apple', 'db', 10000);";
        DataQueryResult result1 = session.executeDataQuery(query1, TxControl.serializableRw().setCommitTx(true))
            .join()
            .getValue();
        DataQueryResults.print(result1);

        String query2 = "SELECT * FROM [" + tablePath + "];";
        DataQueryResult result2 = executeQuery(session, query2);
        System.out.println("Table content:");
        DataQueryResults.print(result2);

        DataQuery query3 = makePreparedQuery(session,
                "DECLARE $id AS Uint32; SELECT id, security FROM [" + tablePath + "] where id=$id;");

        Params params = Params.of("$id", PrimitiveValue.newUint32(1));

        DataQueryResult result3 = query3.execute(TxControl.serializableRw().setCommitTx(true), params)
            .join()
            .getValue();

        if (result3.isEmpty()) {
            throw new IllegalStateException("empty result set");
        }

        System.out.println("Prepared query results:");
        // Index of result set corresponds to its order in YQL query
        ResultSetReader resultSet = result3.getResultSet(0);
        for (int i = 0; i < resultSet.getColumnCount(); i++) {
            System.out.print(resultSet.getColumnName(i) + '\t');
        }
        System.out.println("-------------------------------------");
        while (resultSet.next()) {
            long id = resultSet.getColumn("id").getUint32();
            String security = resultSet.getColumn("security").getText();
            System.out.println(String.format("ID=%d, security=%s", id, security));
        }

        String query4 = "INSERT INTO [" + tablePath +
                "] (id, counterparty, security, amount) VALUES (1, 'Fedex', 'tmob', 'Bad Value');";
        executeQuery(session, query4);
    }

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        final String rootPath = pathPrefix + UUID.randomUUID().toString();
        final String workDirPath = rootPath + "/MyData";
        final String ordersTablePath = workDirPath + "/Orders";

        final SchemeClient schemeClient = SchemeClient.newClient(transport).build();

        try (TableClient tableClient = TableClient.newClient(transport).build()) {
            try (Session session = makeSession(tableClient)) {
                makeDirectory(schemeClient, rootPath);
                makeDirectory(schemeClient, workDirPath);
                createOrdersTable(session, ordersTablePath);

                processBasicData(session, ordersTablePath);

                dropTable(session, ordersTablePath);
                removeDir(schemeClient, workDirPath);
                removeDir(schemeClient, rootPath);
            }
        }
    }

    public static void main(String[] args) {
        new BasicWorkflow().doMain(args);
    }
}
