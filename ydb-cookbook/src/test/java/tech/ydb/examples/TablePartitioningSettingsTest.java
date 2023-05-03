package tech.ydb.examples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.impl.SimpleTableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.PartitioningSettings;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.test.junit5.GrpcTransportExtension;

/**
 *
 * @author Alexandr Gorshenin
 */
public class TablePartitioningSettingsTest {
    @RegisterExtension
    public final GrpcTransportExtension ydb = new GrpcTransportExtension();

    private final String TABLE_NAME = "test1_table";

    private SimpleTableClient tableClient;
    private SessionRetryContext ctx;
    private String tablePath;

    @BeforeEach
    public void before() {
        tableClient =  SimpleTableClient.newClient(GrpcTableRpc.useTransport(ydb)).build();
        ctx = SessionRetryContext.create(tableClient).build();
        tablePath = ydb.getDatabase() + "/" + TABLE_NAME;
    }


    @Test
    public void testPartitioningSettings() {
        PartitioningSettings initSettings = new PartitioningSettings();
        initSettings.setPartitionSize(2500);       // 2000 by default
        initSettings.setMinPartitionsCount(5);     // 1 by default
        initSettings.setMaxPartitionsCount(500);   // 50 by default
        initSettings.setPartitioningByLoad(true);  // false by default
        initSettings.setPartitioningBySize(true);  // true by default

        TableDescription tableDescription = TableDescription.newBuilder()
                .addNullableColumn("id", PrimitiveType.Uint64)
                .addNullableColumn("code", PrimitiveType.Text)
                .addNullableColumn("size", PrimitiveType.Float)
                .addNullableColumn("created", PrimitiveType.Timestamp)
                .addNullableColumn("data", PrimitiveType.Json)
                .setPrimaryKey("id")
                .setPartitioningSettings(initSettings)
                .build();

        PartitioningSettings updateSettings = new PartitioningSettings();
        updateSettings.setMinPartitionsCount(2);
        updateSettings.setMaxPartitionsCount(300);
        updateSettings.setPartitioningByLoad(false);

        PartitioningSettings mergedSettings = new PartitioningSettings();
        mergedSettings.setPartitionSize(2500);       // init value
        mergedSettings.setMinPartitionsCount(2);     // updated value
        mergedSettings.setMaxPartitionsCount(300);   // updated value
        mergedSettings.setPartitioningBySize(true);  // init value
        mergedSettings.setPartitioningByLoad(false); // updated value

        createTable(tableDescription);
        describeTable(tableDescription, initSettings, false);

        alterTable(updateSettings);
        describeTable(tableDescription, mergedSettings, true);

//        dropTable();
    }

    private void createTable(TableDescription tableDescription) {
        Status status = ctx.supplyStatus(
                session -> session.createTable(tablePath, tableDescription, new CreateTableSettings())
        ).join();

        Assertions.assertTrue(status.isSuccess(), "Create table with PartitioningSettings");
    }

    private void describeTable(
            TableDescription tableDescription,
            PartitioningSettings partitioning,
            boolean fetchStats) {

        Result<TableDescription> describeResult = ctx.supplyResult(session -> {
            if (!fetchStats) {
                return session.describeTable(tablePath);
            }

            DescribeTableSettings settings = new DescribeTableSettings();
            settings.setIncludeTableStats(true);
            settings.setIncludePartitionStats(true);
            return session.describeTable(tablePath, settings);
        }).join();
        Assertions.assertTrue(describeResult.isSuccess(), "Describe table");

        TableDescription description = describeResult.getValue();

        Assertions.assertEquals(tableDescription.getColumns().size(), description.getColumns().size(),
                "Table description columns size");

        Assertions.assertEquals(tableDescription.getPrimaryKeys().size(), description.getPrimaryKeys().size(),
                "Table description primary keys size");

        for (int idx = 0; idx < tableDescription.getColumns().size(); idx += 1) {
            TableColumn one = tableDescription.getColumns().get(idx);
            TableColumn two = description.getColumns().get(idx);

            Assertions.assertEquals(one.getName(), two.getName(), "Table column name " + idx);
            Assertions.assertEquals(one.getType(), two.getType(), "Table column type " + idx);
        }

        for (int idx = 0; idx < tableDescription.getPrimaryKeys().size(); idx += 1) {
            String one = tableDescription.getPrimaryKeys().get(idx);
            String two = description.getPrimaryKeys().get(idx);
            Assertions.assertEquals(one, two, "Table primary key " + idx);
        }

        PartitioningSettings settings = description.getPartitioningSettings();
        Assertions.assertNotNull(settings, "Table partitioning settings");

        assert (settings != null);
        Assertions.assertEquals(
                partitioning.getPartitionSizeMb(), settings.getPartitionSizeMb(), "Partition Size Mb");
        Assertions.assertEquals(
                partitioning.getMinPartitionsCount(), settings.getMinPartitionsCount(), "Min Partitions Count");
        Assertions.assertEquals(
                partitioning.getMaxPartitionsCount(), settings.getMaxPartitionsCount(), "Max Partitions Count");
        Assertions.assertEquals(
                partitioning.getPartitioningByLoad(), settings.getPartitioningByLoad(), "Partitioning By Load");
        Assertions.assertEquals(
                partitioning.getPartitioningBySize(), settings.getPartitioningBySize(), "Partitioning By Size");

        if (fetchStats) {
            Assertions.assertNotNull(description.getTableStats(),
                    "Table description table stats are not null");
            Assertions.assertFalse(description.getPartitionStats().isEmpty(),
                    "Table description partition stats are not empty");
        } else {
            Assertions.assertNull(description.getTableStats(),
                    "Table description table stats are null");
            Assertions.assertTrue(description.getPartitionStats().isEmpty(),
                    "Table description partition stats are empty");
        }
    }

    private void alterTable(PartitioningSettings partitioning) {
        AlterTableSettings settings = new AlterTableSettings();
        settings.setPartitioningSettings(partitioning);

        Status status = ctx.supplyStatus(
                session -> session.alterTable(tablePath, settings)
        ).join();

        Assertions.assertTrue(status.isSuccess(), "Alter table with PartitioningSettings");
    }

    private void dropTable() {
        Status status = ctx.supplyStatus(
                session -> session.dropTable(tablePath)
        ).join();

        Assertions.assertTrue(status.isSuccess(), "Drop table with PartitioningSettings");
    }

}
