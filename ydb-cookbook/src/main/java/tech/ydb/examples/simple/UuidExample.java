package tech.ydb.examples.simple;

import java.util.UUID;

import tech.ydb.core.rpc.RpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.transaction.TxControl;


/**
 * @author Sergey Polovko
 */
public class UuidExample extends SimpleExample {

    @Override
    void run(RpcTransport transport, String pathPrefix) {
        TableClient tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport)).build();

        Session session = tableClient.createSession()
            .join()
            .expect("cannot create session");

        String query = "SELECT CAST(\"00112233-4455-6677-8899-aabbccddeeff\" AS Uuid);";
        DataQueryResult result = session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
            .join()
            .expect("query failed");

        ResultSetReader resultSet = result.getResultSet(0);
        resultSet.next();

        UUID uuid = resultSet.getColumn(0).getUuid();
        System.out.println("uuid: " + uuid);

        session.close()
            .join()
            .expect("cannot close session");
    }

    public static void main(String[] args) {
        new UuidExample().doMain();
    }
}
