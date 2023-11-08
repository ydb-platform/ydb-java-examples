
package tech.ydb.example;

import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;

/**
 *
 * @author Aleksandr Gorshenin
 */
public final class Main {

    public static void main(String[] args) {
        if (args.length != 3 && args.length != 2) {
            System.err.println("Usage: java -jar ydb-static-credentials-example.jar <connection-string> <loging> <password>");
            return;
        }
        String connectionString = args[0];
        String username = args[1];
        String password = args.length > 2 ? args[2] : "";

        // Use credentials auth provider with username and password
        StaticCredentials authProvider = new StaticCredentials(username, password);

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
