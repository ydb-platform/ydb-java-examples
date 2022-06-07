package tech.ydb.examples.simple;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;


/**
 * @author Sergey Polovko
 */
public class NameResolverExample {
    private NameResolverExample() { }

    public static void main(String[] args) {
        GrpcTransport transport = GrpcTransport.forEndpoint("ydb-ru.yandex.net", "/ru/home/username/mydb")
            .build();

        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession().join().expect("create session")
                ) {

            DataQueryResult dataResult = session.executeDataQuery("SELECT 1;", TxControl.serializableRw())
                .join()
                .expect("query failed");

            ResultSetReader resultSet = dataResult.getResultSet(0);
            resultSet.next();

            long value = resultSet.getColumn(0).getUint32();
            System.out.println("value=" + value);

        }
    }

}
