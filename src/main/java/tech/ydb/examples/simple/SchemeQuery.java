package tech.ydb.examples.simple;

import tech.ydb.core.rpc.RpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;


/**
 * @author Sergey Polovko
 */
public class SchemeQuery extends SimpleExample {

    @Override
    protected void run(RpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();
        TableClient tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport)).build();

        Session session = tableClient.createSession()
            .join()
            .expect("cannot create session");

        String createTable =
            "CREATE TABLE [" + tablePath + "] (" +
            "  key Uint32," +
            "  value String," +
            "  PRIMARY KEY(key)" +
            ");";

        session.executeSchemeQuery(createTable)
            .join()
            .expect("cannot create table");

        session.executeSchemeQuery("DROP TABLE [" + tablePath + "];")
            .join()
            .expect("cannot drop table");

        session.close()
            .join()
            .expect("cannot close session");
    }

    public static void main(String[] args) {
        new SchemeQuery().doMain();
    }
}
