package tech.ydb.examples.pagination;

import java.util.ArrayList;
import java.util.List;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.App;
import tech.ydb.examples.AppRunner;
import tech.ydb.examples.pagination.model.School;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;


/**
 * @author Sergey Polovko
 * <p>
 * To organize paginated output, we recommend selecting data sorted by primary key sequentially, limiting the number
 * of rows with the LIMIT keyword.
 * </p>
 */
public class PaginationApp implements App {
    private static final int MAX_PAGES = 10;
    private final String path;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;

    PaginationApp(GrpcTransport transport, String path) {
        this.path = path;
        this.tableClient = TableClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    public static void main(String[] args) {
        AppRunner.run("PaginationApp", PaginationApp::new, args);
    }

    public static int test(String[] args) {
        return AppRunner.safeRun("PaginationApp", PaginationApp::new, args);
    }

    @Override
    public void run() {
        createTable();
        describeTable();
        fillTableDataTransaction();

        final long limit = 3;

        System.out.println("--[ Pagination ] -----------------------");
        System.out.println("limit: " + limit);

        int page = 0;
        School.Key lastKey = new School.Key("", 0);
        while (page <= MAX_PAGES) {
            ++page;
            List<School> schools = selectPaging(limit, lastKey);
            if (schools.isEmpty()) {
                break;
            }

            System.out.println("-[ page: " + page + " ]-");
            for (School school : schools) {
                System.out.println("    " + school);
            }

            School lastSchool = schools.get(schools.size() - 1);
            lastKey = lastSchool.toKey();
        }
    }

    /**
     * Creates sample tables with CrateTable API.
     */
    private void createTable() {
        TableDescription schoolTable = TableDescription.newBuilder()
                .addNullableColumn("city", PrimitiveType.Text)
                .addNullableColumn("number", PrimitiveType.Uint32)
                .addNullableColumn("address", PrimitiveType.Text)
                .setPrimaryKeys("city", "number")
                .build();

        retryCtx.supplyStatus(session -> session.createTable(path + "/schools", schoolTable))
                .join()
                .expectSuccess("cannot create schools table");
    }

    /**
     * Describe existing table.
     */
    private void describeTable() {
        System.out.println("--[ DescribeTable ]---------------------------------------");

        String tablePath = path + "/schools";
        TableDescription tableDesc = retryCtx.supplyResult(session -> session.describeTable(tablePath))
                .join()
                .getValue();

        System.out.println(tablePath + ':');
        List<String> primaryKeys = tableDesc.getPrimaryKeys();
        for (TableColumn column : tableDesc.getColumns()) {
            boolean isPrimary = primaryKeys.contains(column.getName());
            System.out.println("    " + column.getName() + ": " + column.getType() + (isPrimary ? " (PK)" : ""));
        }
        System.out.println();
    }

    /**
     * Fills sample table with data in single parameterized data query.
     */
    private void fillTableDataTransaction() {
        String query =
                "DECLARE $schoolsData AS List<Struct<\n" +
                        "    city: Text,\n" +
                        "    number: Uint32,\n" +
                        "    address: Text>>;\n" +
                        "\n" +
                        "REPLACE INTO schools\n" +
                        "SELECT\n" +
                        "    city,\n" +
                        "    number,\n" +
                        "    address\n" +
                        "FROM AS_TABLE($schoolsData);";

        Params params = Params.of("$schoolsData", PaginationData.SCHOOL_DATA);
        TxControl<?> txControl = TxControl.serializableRw().setCommitTx(true);

        retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl, params))
                .join()
                .getValue();
    }

    private List<School> selectPaging(long limit, School.Key lastSchool) {
        String query =
                "DECLARE $limit AS Uint64;\n" +
                        "DECLARE $lastCity AS Text;\n" +
                        "DECLARE $lastNumber AS Uint32;\n" +
                        "\n" +
                        "SELECT * FROM schools\n" +
                        "WHERE (city, number) > ($lastCity, $lastNumber)\n" +
                        "ORDER BY city, number\n" +
                        "LIMIT $limit;";

        /* In the query example shown above, the WHERE clause uses a tuple comparison to select the next set of rows.
         Tuples are compared element by element from left to right, so the order of the fields in the tuple must
         match the order of the fields in the primary key to avoid table full scan. */

        Params params = Params.of(
                "$limit", PrimitiveValue.newUint64(limit),
                "$lastCity", PrimitiveValue.newText(lastSchool.getCity()),
                "$lastNumber", PrimitiveValue.newUint32(lastSchool.getNumber()));

        TxControl<?> txControl = TxControl.serializableRw().setCommitTx(true);

        DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl, params))
                .join()
                .getValue();

        ResultSetReader resultSet = result.getResultSet(0);
        List<School> schools = new ArrayList<>(resultSet.getRowCount());

        while (resultSet.next()) {
            String city = resultSet.getColumn("city").getText();
            int number = (int) resultSet.getColumn("number").getUint32();
            String address = resultSet.getColumn("address").getText();
            schools.add(new School(city, number, address));
        }
        return schools;
    }

    @Override
    public void close() {
        tableClient.close();
    }
}
