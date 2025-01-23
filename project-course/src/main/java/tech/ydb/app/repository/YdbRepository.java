package tech.ydb.app.repository;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.result.ResultSetReader;

/**
 * @author Kirill Kurdyukov
 */
public class YdbRepository {
    private final SessionRetryContext retryCtx;

    public YdbRepository(String connectionString) {
        GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
        QueryClient queryClient = QueryClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(queryClient).build();
    }

    public void TestDatabase() {
        QueryReader resultSet = retryCtx.supplyResult(session ->
                QueryReader.readFrom(session.createQuery("SELECT 1;", TxMode.NONE))
        ).join().getValue();

        ResultSetReader resultSetReader = resultSet.getResultSet(0);

        if (resultSetReader.next()) {
            System.out.println("Database is available!");
        }
    }
}
