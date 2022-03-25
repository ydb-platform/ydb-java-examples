package tech.ydb.example;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.AuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.transaction.TxControl;

import java.time.Duration;

public final class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar ydb-environ-example.jar <connection-string>");
            return;
        }
        String connectionString = args[0];

        // Construct authProvider from environment variables
        AuthProvider authProvider = CloudAuthHelper.getAuthProviderFromEnviron();

        GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build();

        try (TableClient tableClient = TableClient
                .newClient(GrpcTableRpc.ownTransport(transport))
                .build()) {

            Session session = tableClient.getOrCreateSession(Duration.ofSeconds(10))
                    .join().expect("ok");

            ResultSetReader rsReader = session.executeDataQuery("SELECT 1;", TxControl.serializableRw())
                    .join().expect("ok").getResultSet(0);

            assert (rsReader.getRowCount() == 1);
            System.out.println("Result:");
            while (rsReader.next()) {
                System.out.println(rsReader.getColumn(0).getInt32());
            }

            session.release();
        }
    }
}
