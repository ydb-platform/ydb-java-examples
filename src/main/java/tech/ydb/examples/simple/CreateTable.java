package tech.ydb.examples.simple;

import tech.ydb.core.rpc.RpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.PartitioningPolicy;
import tech.ydb.table.types.PrimitiveType;


/**
 * @author Sergey Polovko
 */
public class CreateTable extends SimpleExample {

    @Override
    void run(RpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();
        TableClient tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport)).build();

        Session session = tableClient.createSession()
            .join()
            .expect("cannot create session");

        session.dropTable(tablePath)
            .join();

        {
            TableDescription description = TableDescription.newBuilder()
                .addNullableColumn("hash", PrimitiveType.uint32())
                .addNullableColumn("name", PrimitiveType.utf8())
                .addNullableColumn("salary", PrimitiveType.float64())
                .setPrimaryKeys("hash", "name")  // uniform partitioning requires Uint32 / Uint64 as a first key column
                .build();

            CreateTableSettings settings = new CreateTableSettings()
                .setPartitioningPolicy(new PartitioningPolicy().setUniformPartitions(4));

            session.createTable(tablePath, description, settings)
                .join()
                .expect("cannot create table");
        }

        TableDescription description = session.describeTable(tablePath)
            .join()
            .expect("cannot describe table");

        System.out.println("--[primary keys]-------------");
        int i = 1;
        for (String primaryKey : description.getPrimaryKeys()) {
            System.out.printf("%4d. %s\n", i++, primaryKey);
        }

        System.out.println("\n--[columns]------------------");
        i = 1;
        for (TableColumn column : description.getColumns()) {
            System.out.printf("%4d. %s %s\n", i++, column.getName(), column.getType());
        }

        session.close()
            .join()
            .expect("cannot close session");
    }

    public static void main(String[] args) {
        new CreateTable().doMain();
    }
}
