package tech.ydb.slo.core;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.beust.jcommander.JCommander;
import org.junit.jupiter.api.Test;

import tech.ydb.slo.core.kv.KvClient;
import tech.ydb.slo.core.kv.KvSession;
import tech.ydb.slo.core.kv.KvWorkloadParams;
import tech.ydb.slo.core.kv.OpOutcome;
import tech.ydb.slo.core.kv.Row;
import tech.ydb.slo.core.kv.WorkloadRunner;

import static org.junit.jupiter.api.Assertions.*;

class WorkloadRunnerTest {
    @Test
    void checksControlGenerationsWhileLoadIsActive() throws Exception {
        KvWorkloadParams params = new KvWorkloadParams();
        JCommander.newBuilder().addObject(params).build().parse(
                "--read-rps", "0", "--write-rps", "1", "--prefill-count", "0",
                "--duration", "0", "--max-workers", "1");
        AtomicReference<Row> stored = new AtomicReference<>();
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch checkedDuringLoad = new CountDownLatch(1);
        KvClient client = new KvClient() {
            public void createTable(KvWorkloadParams options, String tablePath) {}
            public void dropTable(String tablePath) {}
            public KvSession openSession() {
                return new KvSession() {
                    public OpOutcome write(Row row, int timeoutMs) {
                        if (row.id() == Long.MAX_VALUE) {
                            stored.set(row);
                        } else {
                            loadStarted.countDown();
                        }
                        return OpOutcome.success(0);
                    }
                    public OpOutcome read(long id, int timeoutMs) {
                        Row row = stored.get();
                        if (row.payloadHash() >= 2) {
                            try {
                                assertTrue(loadStarted.await(10, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(e);
                            }
                            checkedDuringLoad.countDown();
                            row = new Row(row.id(), row.payloadStr(), row.payloadDouble(),
                                    row.payloadTimestamp(), row.payloadHash() - 1);
                        }
                        return OpOutcome.success(0, List.of(row));
                    }
                };
            }
        };
        Config config = new Config("grpc://localhost:1/local", "", "", "unit",
                "control-runner", 0, "");
        try (Metrics metrics = Metrics.create(config)) {
            WorkloadRunner runner = new WorkloadRunner(client, metrics, params, "unused");
            runner.setup();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<IllegalStateException> result = executor.submit(
                    () -> assertThrows(IllegalStateException.class, runner::run));
            try {
                assertTrue(checkedDuringLoad.await(10, TimeUnit.SECONDS));
                IllegalStateException failure = result.get(10, TimeUnit.SECONDS);
                assertInstanceOf(IllegalStateException.class, failure.getCause());
                assertEquals(0, loadStarted.getCount());
                assertEquals(2, stored.get().payloadHash());
            } finally {
                result.cancel(true);
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }
}
