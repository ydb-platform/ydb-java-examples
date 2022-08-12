package tech.ydb.example;

import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

import tech.ydb.auth.iam.CloudAuthProvider;
import tech.ydb.core.Result;
import tech.ydb.core.auth.AuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;

import yandex.cloud.sdk.auth.provider.ApiKeyCredentialProvider;

public final class Main {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java -jar ydb-service-account-example <connection-string> <sa-key-file>");
            return;
        }
        String connectionString = args[0];
        String saKeyFile = args[1];

        AuthProvider authProvider = CloudAuthProvider.newAuthProvider(
            ApiKeyCredentialProvider.builder()
                .fromFile(Paths.get(saKeyFile))
                .build()
        );

        try ( GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider) // Or this method could not be called at all
                .build()) {
            try ( TableClient tableClient = TableClient
                    .newClient(transport)
                    .build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

                retryCtx.supplyResult(session -> {
                    ResultSetReader rsReader = session.executeDataQuery("SELECT 1;", TxControl.serializableRw())
                            .join().getValue().getResultSet(0);

                    rsReader.next();
                    System.out.println(rsReader.getColumn(0).getInt32());

                    return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
                });
            }
        }
    }
}
