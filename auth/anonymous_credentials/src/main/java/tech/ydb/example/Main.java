package tech.ydb.example;



import java.time.Duration;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;

public final class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar ydb-anonimous-example.jar <connection-string>");
            return;
        }
        String connectionString = args[0];

        // Anonymous credentials
        AuthProvider authProvider = NopAuthProvider.INSTANCE;

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .withGrpcKeepAliveTime(Duration.ofSeconds(10))
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

                for (int step = 0; step < 1800; step++) {
                    System.out.println("" + step);
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
}
