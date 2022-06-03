package tech.ydb.examples.simple;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.DecimalValue;


/**
 * @author Sergey Polovko
 */
public class DecimalExample extends SimpleExample {

    @Override
    void run(GrpcTransport transport, String pathPrefix) {
        TableClient tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport)).build();

        Session session = tableClient.createSession()
            .join()
            .expect("cannot create session");

        String query = "SELECT CAST(\"12.345\" AS Decimal(13, 3));";
        DataQueryResult result = session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
            .join()
            .expect("query failed");

        ResultSetReader resultSet = result.getResultSet(0);
        resultSet.next();

        DecimalValue value = resultSet.getColumn(0).getDecimal();
        System.out.println("decimal: " + value);
        System.out.println("BigDecimal: " + value.toBigDecimal());

        session.close()
            .join()
            .expect("cannot close session");
    }

    public static void main(String[] args) {
        new DecimalExample().doMain();
    }
}
