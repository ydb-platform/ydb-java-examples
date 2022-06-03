package tech.ydb.examples.simple;

import java.time.Duration;

import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.transaction.TxControl;

import static tech.ydb.table.values.PrimitiveValue.uint32;


/**
 * @author Sergey Polovko
 */
public class ReadTableExample extends SimpleExample {

    @Override
    void run(GrpcTransport transport, String pathPrefix) {
        try (TableClient tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport))
                .sessionPoolSize(10, 20)
                .build()) {
            Session session = tableClient.getOrCreateSession(Duration.ofSeconds(3))
                .join()
                .expect("cannot get session");

            String tablePath = pathPrefix + getClass().getSimpleName();
            createAndFillTable(session, tablePath);
            readTable(session, tablePath);
        }
    }

    private void readTable(Session session, String tablePath) {
        ReadTableSettings settings = ReadTableSettings.newBuilder()
            .orderedRead(true)
            .fromKeyInclusive(uint32(10))
            .toKeyExclusive(uint32(25))
            .build();

        Status status = session.readTable(tablePath, settings, resultSet -> {
            // we are going to read a lot of data, so map column names to indexes
            // outside of the loop to avoid overhead on each loop iteration
            int keyIdx = resultSet.getColumnIndex("key");
            int valueIdx = resultSet.getColumnIndex("value");

            while (resultSet.next()) {
                long key = resultSet.getColumn(keyIdx).getUint32();
                String value = resultSet.getColumn(valueIdx).getUtf8();
                System.out.printf("key=%d, value=%s\n", key, value);
            }
        }).join();
        status.expect("readTable failed");
    }

    private void createAndFillTable(Session session, String tablePath) {
        String createTable =
            "CREATE TABLE [" + tablePath + "] (" +
            "  key Uint32," +
            "  value Utf8," +
            "  PRIMARY KEY(key)" +
            ");";

        session.executeSchemeQuery(createTable)
            .join()
            .expect("cannot create table");

        for (int i = 0; i < 100; i++) {
            String query = "REPLACE INTO [" + tablePath + "](key, value) VALUES (" + i + ", \"<" + i + ">\");";
            session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
                .join()
                .expect("cannot execute insert");
        }
    }

    public static void main(String[] args) {
        new ReadTableExample().doMain();
    }
}
