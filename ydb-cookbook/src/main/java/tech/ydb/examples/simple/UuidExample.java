package tech.ydb.examples.simple;

import java.time.Duration;
import java.util.UUID;
import tech.ydb.core.grpc.GrpcTransport;

import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;


/**
 * @author Sergey Polovko
 */
public class UuidExample extends SimpleExample {

    @Override
    void run(GrpcTransport transport, String pathPrefix) {
        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession(Duration.ofSeconds(5)).join().expect("create session")
                ) {
            String query = "SELECT CAST(\"00112233-4455-6677-8899-aabbccddeeff\" AS Uuid);";
            DataQueryResult result = session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
                    .join()
                    .expect("query failed");
            
            ResultSetReader resultSet = result.getResultSet(0);
            resultSet.next();
            
            UUID uuid = resultSet.getColumn(0).getUuid();
            System.out.println("uuid: " + uuid);
        }
    }

    public static void main(String[] args) {
        new UuidExample().doMain();
    }
}
