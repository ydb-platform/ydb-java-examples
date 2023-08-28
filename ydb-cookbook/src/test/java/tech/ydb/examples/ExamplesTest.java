package tech.ydb.examples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.examples.batch_upload.BatchUpload;
import tech.ydb.examples.bulk_upsert.BulkUpsert;
import tech.ydb.examples.pagination.PaginationApp;
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

    @Test
    public void testBatchUpload() {
        Assertions.assertEquals(0, BatchUpload.test(args()), "Batch upload test");
    }

    @Test
    public void testBulkUpsert() {
        Assertions.assertEquals(0, BulkUpsert.test(args()), "Bulk upsert test");
    }

    @Test
    public void testPagination() {
        Assertions.assertEquals(0, PaginationApp.test(args(), "Pagination test"));
    }
}
