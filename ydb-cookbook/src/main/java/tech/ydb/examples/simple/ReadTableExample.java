package tech.ydb.examples.simple;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;


/**
 * @author Sergey Polovko
 */
public class ReadTableExample extends SimpleExample {
    private static final String TABLE_NAME = "read_table_example";
    private static final Logger logger = LoggerFactory.getLogger(ReadTableExample.class);


    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        try (TableClient tableClient = TableClient.newClient(transport).build()) {
            SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

            createAndFillTable(retryCtx);
            readTable(retryCtx, transport.getDatabase());
            dropTable(retryCtx);
        }
    }

    private void readTable(SessionRetryContext retryCtx, String database) {

        ReadTableSettings settings = ReadTableSettings.newBuilder()
            .orderedRead(true)
            .fromKeyInclusive(PrimitiveValue.newUint32(10))
            .toKeyExclusive(PrimitiveValue.newUint32(25))
            .build();

        String tablePath = database + "/" + TABLE_NAME;
        retryCtx.supplyStatus(session -> session.executeReadTable(tablePath, settings).start(part -> {
            ResultSetReader resultSet = part.getResultSetReader();

            // we are going to read a lot of data, so map column names to indexes
            // outside of the loop to avoid overhead on each loop iteration
            int keyIdx = resultSet.getColumnIndex("key");
            int valueIdx = resultSet.getColumnIndex("value");

            while (resultSet.next()) {
                long key = resultSet.getColumn(keyIdx).getUint32();
                String value = resultSet.getColumn(valueIdx).getText();
                logger.info("key={}, value={}", key, value);
            }
        })).join().expectSuccess("readTable failed");
    }

    private void createAndFillTable(SessionRetryContext retryCtx) {
        String createTable =
            "CREATE TABLE " + TABLE_NAME + " (" +
            "  key Uint32," +
            "  value Utf8," +
            "  PRIMARY KEY(key)" +
            ");";

        retryCtx.supplyStatus(session -> session.executeSchemeQuery(createTable))
            .join()
            .expectSuccess("cannot create table");

        for (int i = 0; i < 100; i++) {
            String query = "UPSERT INTO " + TABLE_NAME + "(key, value) VALUES (" + i + ", \"<" + i + ">\");";
            retryCtx.supplyResult(session -> session.executeDataQuery(query, TxControl.serializableRw()))
                    .join().getStatus().expectSuccess("cannot execute insert");
        }
    }

    private void dropTable(SessionRetryContext retryCtx) {
        String dropSQL = "DROP TABLE " + TABLE_NAME + ";";
        retryCtx.supplyStatus(session -> session.executeSchemeQuery(dropSQL))
            .join()
            .expectSuccess("cannot create table");
    }

    public static void main(String[] args) {
        new ReadTableExample().doMain(args);
    }
}
