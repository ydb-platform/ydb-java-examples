package tech.ydb.examples.simple;


import java.util.Arrays;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.App;
import tech.ydb.examples.AppRunner;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.ReadRowsResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.ReadRowsSettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ReadRowsApp implements App {
    private static final Logger log = LoggerFactory.getLogger(ReadRowsApp.class);

    private static final String TABLE_NAME = "readrows_test";

    private final String path;
    private final String tablePath;

    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;

    ReadRowsApp(GrpcTransport transport, String path) {
        this.path = path;
        this.tableClient = TableClient.newClient(transport).build();
        this.tablePath = this.path + "/" + TABLE_NAME;
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    private void createTables() {
        log.info("drop table {} if exist", tablePath);
        boolean dropped = retryCtx
                .supplyStatus(session -> session.dropTable(tablePath))
                .join().isSuccess();
        if (!dropped) {
            log.info("drop table failed");
        }

        TableDescription table = TableDescription.newBuilder()
                .addNonnullColumn("id", PrimitiveType.Uint32)
                .addNullableColumn("text", PrimitiveType.Text)
                .addNullableColumn("payload", PrimitiveType.Bytes)
                .setPrimaryKey("id")
                .build();

        log.info("create table {}", tablePath);
        retryCtx.supplyStatus(session -> session.createTable(tablePath, table))
                .join().expectSuccess("create table fail");
    }

    private void writeData() {
        ListValue bulkData = Record.makeYdbBulk(
                new Record(1, "id1", new byte[]{0, 1, 2}),
                new Record(2, "id2", new byte[]{1, 1, 1, 1, 1}),
                new Record(3, "", new byte[]{}),
                new Record(4, null, null)
        );

        retryCtx.supplyStatus(session -> session.executeBulkUpsert(
                tablePath, bulkData, new BulkUpsertSettings()
        )).join().expectSuccess("bulk upsert problem");
    }

    private void readRows() {
        ReadRowsSettings settings = ReadRowsSettings.newBuilder().addKeys(
                StructValue.of("id", PrimitiveValue.newUint32(1)),
                StructValue.of("id", PrimitiveValue.newUint32(2)),
                StructValue.of("id", PrimitiveValue.newUint32(3)),
                StructValue.of("id", PrimitiveValue.newUint32(4))
        ).build();

        Result<ReadRowsResult> result = retryCtx.supplyResult(session -> session.readRows(tablePath, settings))
                .join();

        ResultSetReader rs = result.getValue().getResultSetReader();
        while (rs.next()) {
            log.info("Readed row " + new Record(rs));
        }
    }

    private void readTable() {
        ReadTableSettings settings = ReadTableSettings.newBuilder().orderedRead(true).build();
        retryCtx.supplyStatus(session -> session.executeReadTable(tablePath, settings).start(part -> {
            ResultSetReader rs = part.getResultSetReader();
            while (rs.next()) {
                log.info("Readed from table " + new Record(rs));
            }
        })).join().expectSuccess();
    }

    @Override
    public void run() {
        createTables();
        writeData();
        readRows();
        readTable();
    }

    @Override
    public void close() {
        tableClient.close();
    }

    public static int test(String[] args) {
        return AppRunner.safeRun("ReadRowsApp", ReadRowsApp::new, args);
    }

    public static void main(String[] args) {
        AppRunner.run("ReadRowsApp", ReadRowsApp::new, args);
    }

    private static <T> T readColumn(ResultSetReader rs, String name, Function<ValueReader, T> method) {
        Type type = rs.getColumnType(rs.getColumnIndex(name));
        ValueReader reader = rs.getColumn(name);
        if (type.getKind() == Type.Kind.OPTIONAL && !reader.isOptionalItemPresent()) {
            return null;
        }
        return method.apply(reader);
    }

    private static class Record {
        private static final StructType TYPE = StructType.of(
                "id", PrimitiveType.Uint32,
                "text", PrimitiveType.Text,
                "payload", PrimitiveType.Bytes
        );

        private final long id;
        private final String text;
        private final byte[] payload;

        Record(long id, String text, byte[] payload) {
            this.id = id;
            this.text = text;
            this.payload = payload;
        }

        Record(ResultSetReader rs) {
            this.id = readColumn(rs, "id", ValueReader::getUint32);
            this.text = readColumn(rs, "text", ValueReader::getText);
            this.payload = readColumn(rs, "payload", ValueReader::getBytes);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Record[id=");
            sb.append(id);
            if (text != null) {
                sb.append(", text='").append(text).append("'");
            }
            if (payload != null) {
                sb.append(", payload=").append(Arrays.toString(payload));
            }
            return sb.append("]").toString();
        }

        public Value<?> toYdbValue() {
            return TYPE.newValue(
                    "id", PrimitiveValue.newUint32(id),
                    "text", text == null ? VoidValue.of() : PrimitiveValue.newText(text),
                    "payload", payload == null ? VoidValue.of() : PrimitiveValue.newBytes(payload)
            );
        }

        public static ListValue makeYdbBulk(Record... records) {
            Value<?>[] values = new Value<?>[records.length];
            for (int idx = 0; idx < records.length; idx += 1) {
                log.info("Write record {} ", records[idx]);
                values[idx] = records[idx].toYdbValue();
            }
            return ListType.of(TYPE).newValueOwn(values);
        }
    }
}
