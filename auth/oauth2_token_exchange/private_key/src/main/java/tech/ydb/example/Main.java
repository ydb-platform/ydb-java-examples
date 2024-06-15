package tech.ydb.example;


import java.io.File;

import tech.ydb.auth.OAuth2TokenExchangeProvider;
import tech.ydb.auth.OAuth2TokenSource;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;


public final class Main {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java -jar ydb-oauth2-private-key-example "
                    + "<connection-string> <oauth2-endpoint> <rsa-private-key.pem>");
            return;
        }
        String connectionString = args[0];
        String oauth2Endpoint = args[1];
        String keyPemPath = args[2];

        OAuth2TokenSource tokenSource = OAuth2TokenSource.withPrivateKeyPemFile(new File(keyPemPath))
                .withIssuer("test-issuer") // customize of JWT token
                .build();

        OAuth2TokenExchangeProvider authProvider = OAuth2TokenExchangeProvider.newBuilder(oauth2Endpoint, tokenSource)
                .withScope("demo-scope") // customize of OAuth2 request
                .build();

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
