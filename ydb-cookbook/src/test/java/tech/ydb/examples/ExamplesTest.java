package tech.ydb.examples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.examples.batch_upload.BatchUpload;
import tech.ydb.examples.bulk_upsert.BulkUpsert;
import tech.ydb.examples.pagination.PaginationApp;
import tech.ydb.examples.simple.ComplexTransaction;
import tech.ydb.examples.simple.ReadRowsApp;
import tech.ydb.examples.simple.ReadTableExample;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Alexandr Gorshenin
 */
public class ExamplesTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private String[] args() {
        return new String[] {
            "-d", ydb.database(),
            "-e", ydb.endpoint()
        };
    }

    private String[] connectionString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ydb.useTls() ? "grpcs://" : "grpc://" );
        sb.append(ydb.endpoint());
        sb.append(ydb.database());
        return new String [] { sb.toString() };
    }

    @Test
    public void testBatchUpload() {
        Assertions.assertEquals(0, BatchUpload.test(args()), "Batch upload test");
    }

    @Test
    public void testBulkUpsert() {
        Assertions.assertEquals(0, BulkUpsert.test(args()), "Bulk upsert test");
    }

    @Test
    public void testReadTable() {
        ReadTableExample.main(connectionString());
    }

    @Test
    public void testPagination() {
        Assertions.assertEquals(0, PaginationApp.test(args()), "Pagination test");
    }

    @Test
    public void testReadRows() {
        Assertions.assertEquals(0, ReadRowsApp.test(args()), "Read rows test");
    }

    @Test
    public void testComplexTransaction() {
        Assertions.assertEquals(0, ComplexTransaction.test(connectionString()));
    }
}
