package tech.ydb.examples.simple;


import java.time.Duration;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;


/**
 * @author Sergey Polovko
 */
public class ReadTableExample extends SimpleExample {

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        try (
                TableClient tableClient = TableClient.newClient(transport).build();
                Session session = tableClient.createSession(Duration.ofSeconds(5)).join().getValue()
                ) {

            String tablePath = pathPrefix + getClass().getSimpleName();
            createAndFillTable(session, tablePath);
            readTable(session, tablePath);
        }
    }

    private void readTable(Session session, String tablePath) {
        ReadTableSettings settings = ReadTableSettings.newBuilder()
            .orderedRead(true)
            .fromKeyInclusive(PrimitiveValue.newUint32(10))
            .toKeyExclusive(PrimitiveValue.newUint32(25))
            .build();

        session.readTable(tablePath, settings).start(resultSet -> {
            // we are going to read a lot of data, so map column names to indexes
            // outside of the loop to avoid overhead on each loop iteration
            int keyIdx = resultSet.getColumnIndex("key");
            int valueIdx = resultSet.getColumnIndex("value");

            while (resultSet.next()) {
                long key = resultSet.getColumn(keyIdx).getUint32();
                String value = resultSet.getColumn(valueIdx).getText();
                System.out.printf("key=%d, value=%s\n", key, value);
            }
        }).join().expectSuccess("readTable failed");
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
            .expectSuccess("cannot create table");

        for (int i = 0; i < 100; i++) {
            String query = "REPLACE INTO [" + tablePath + "](key, value) VALUES (" + i + ", \"<" + i + ">\");";
            session.executeDataQuery(query, TxControl.serializableRw().setCommitTx(true))
                .join()
                .getStatus().expectSuccess("cannot execute insert");
        }
    }

    public static void main(String[] args) {
        new ReadTableExample().doMain(args);
    }
}
