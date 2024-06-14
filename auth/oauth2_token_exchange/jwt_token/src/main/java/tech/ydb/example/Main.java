package tech.ydb.example;


import tech.ydb.auth.OAuth2Token;
import tech.ydb.auth.OAuth2TokenExchangeProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;


public final class Main {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java -jar ydb-oauth2-token-example "
                    + "<connection-string> <oauth2-endpoint> <oauth2-token-value>");
            return;
        }
        String connectionString = args[0];
        String oauth2Endpoint = args[1];
        String refreshToken = args[2];

        OAuth2Token token = OAuth2Token.fromValue(refreshToken);
        OAuth2TokenExchangeProvider authProvider = OAuth2TokenExchangeProvider.newBuilder(oauth2Endpoint, token)
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
