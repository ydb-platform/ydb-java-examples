package tech.ydb.examples.topic.transactions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.topic.SimpleTopicExample;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.SendSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

/**
 * @author Nikolay Perfilov
 */
public class TransactionWriteSync extends SimpleTopicExample {
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    @Override
    protected void run(GrpcTransport transport) {
        // WARNING: Working with transactions in Java Topic SDK is currently experimental. Interfaces may change
        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {
            try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
                long id = 1;
                String randomProducerId = "randomProducerId"; // Different for writers with different transactions
                WriterSettings writerSettings = WriterSettings.newBuilder()
                        .setTopicPath(TOPIC_NAME)
                        .setProducerId(randomProducerId)
                        .setMessageGroupId(randomProducerId)
                        .build();

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
                retryCtx.supplyStatus(querySession -> {
                    QueryTransaction transaction = querySession.beginTransaction(TxMode.SERIALIZABLE_RW)
                            .join().getValue();
                    String value;
                    QueryStream queryStream = transaction.createQuery(
                            "DECLARE $id AS Uint64;\n" +
                                    "SELECT value FROM table WHERE id=$id",
                            Params.of("$id", PrimitiveValue.newUint64(1)));
                    QueryReader queryReader = QueryReader.readFrom(queryStream).join().getValue();
                    ResultSetReader resultSet = queryReader.getResultSet(0);
                    if (!resultSet.next()) {
                        throw new RuntimeException("Value for id=" + id + " not found");
                    }
                    value = resultSet.getColumn("value").getText();

                    // Current implementation requires creating a writer for every transaction:
                    SyncWriter writer = topicClient.createSyncWriter(writerSettings);
                    writer.initAndWait();
                    writer.send(
                            Message.of(value.getBytes()),
                            SendSettings.newBuilder()
                                    .setTransaction(transaction)
                                    .build()
                    );

                    writer.flush();

                    CompletableFuture<Status> commitStatus = transaction.commit().thenApply(Result::getStatus);
                    commitStatus.join();

                    try {
                        writer.shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } catch (TimeoutException exception) {
                        throw new RuntimeException("Shutdown not finished within " + SHUTDOWN_TIMEOUT_SECONDS +
                                " seconds");
                    } catch (InterruptedException | ExecutionException exception) {
                        throw new RuntimeException("Shutdown not finished due to exception: " + exception);
                    }

                    return commitStatus;
                }).join().expectSuccess("Couldn't read from table and write to topic in transaction");
            }
        }
    }

    public static void main(String[] args) {
        new TransactionWriteSync().doMain(args);
    }
}
