package tech.ydb.examples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.examples.batch_upload.BatchUpload;
import tech.ydb.examples.bulk_upsert.BulkUpsert;
import tech.ydb.test.junit5.YdbHelperExtention;

/**
 *
 * @author Alexandr Gorshenin
 */
public class ExamplesTest {
    @RegisterExtension
    private static final YdbHelperExtention ydb = new YdbHelperExtention();

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
}
