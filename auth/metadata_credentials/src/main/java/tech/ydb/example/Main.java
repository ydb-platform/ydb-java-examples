package tech.ydb.example;


import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;


public final class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar ydb-metadata-example.jar <connection-string>");
            return;
        }
        String connectionString = args[0];

        // Use metadata credentials
        AuthProvider authProvider = CloudAuthHelper.getMetadataAuthProvider();

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build()) {
            try (TableClient tableClient = TableClient.newClient(transport).build()) {
                SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

                DataQueryResult dataQueryResult = retryCtx.supplyResult(
                        session -> session.executeDataQuery("SELECT 1;", TxControl.serializableRw())
                ).join().getValue();

                ResultSetReader rsReader = dataQueryResult.getResultSet(0);
                while (rsReader.next()) {
                    System.out.println(rsReader.getColumn(0).getInt32());
                }
            }
        }
    }
}
