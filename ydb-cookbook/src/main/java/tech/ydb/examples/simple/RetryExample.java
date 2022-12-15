package tech.ydb.examples.simple;

import java.util.concurrent.ForkJoinPool;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.transaction.TxControl;


/**
 * @author Sergey Polovko
 */
public class RetryExample extends SimpleExample {

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        try (TableClient tableClient = TableClient.newClient(transport).build()) {
            SessionRetryContext ctx = SessionRetryContext.create(tableClient)
                .executor(ForkJoinPool.commonPool())
                .maxRetries(5)
                .build();

            Result<DataQueryResult> result = ctx.supplyResult(session -> {
                TxControl txControl = TxControl.serializableRw()
                    .setCommitTx(true);
                return session.executeDataQuery("SELECT 1 + 2;", txControl);
            }).join();

            DataQueryResult dataQueryResult = result.getValue();
            ResultSetReader resultSet = dataQueryResult.getResultSet(0);
            while (resultSet.next()) {
                ValueReader reader = resultSet.getColumn(0);
                System.out.println("result = " + reader.toString());
            }
        }
    }

    public static void main(String[] args) {
        new RetryExample().doMain(args);
    }
}
