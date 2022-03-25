package tech.ydb.example;

import tech.ydb.auth.iam.CloudAuthProvider;
import tech.ydb.core.auth.AuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.TableClient;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.transaction.TxControl;

import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Result;
import tech.ydb.table.SessionRetryContext;
import yandex.cloud.sdk.auth.provider.IamTokenCredentialProvider;

public final class Main {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java -jar ydb-access-token-example.jar <connection-string> <access-token>");
            return;
        }
        String connectionString = args[0];
        String accessToken = args[1];

        // Access token credentials
        AuthProvider authProvider = CloudAuthProvider.newAuthProvider(
            IamTokenCredentialProvider.builder()
                .token(accessToken)
                .build()
        );

        GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider) // Or this method could not be called at all
                .build();

        try (TableClient tableClient = TableClient
                .newClient(GrpcTableRpc.ownTransport(transport))
                .build()) {

            SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

            retryCtx.supplyResult(session -> {
                ResultSetReader rsReader = session.executeDataQuery("SELECT 1;", TxControl.serializableRw())
                        .join().expect("ok").getResultSet(0);

                rsReader.next();
                System.out.println(rsReader.getColumn(0).getInt32());

                return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
            });
        }
    }
}
