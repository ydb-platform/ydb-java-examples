package tech.ydb.examples.topic.transactions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.settings.SendSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

/**
 * @author Nikolay Perfilov
 */
public class TransactionWriteSync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(TransactionWriteSync.class);
    private static final String PRODUCER_ID = "messageGroup1";
    private static final String MESSAGE_GROUP_ID = "messageGroup1";
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {
            try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
                WriterSettings writerSettings = WriterSettings.newBuilder()
                        .setTopicPath(TOPIC_NAME)
                        .setProducerId(PRODUCER_ID)
                        .setMessageGroupId(MESSAGE_GROUP_ID)
                        .build();

                writeFromTableToTopic(topicClient, queryClient, writerSettings, 1);
                writeFromTableToTopic(topicClient, queryClient, writerSettings, 2);
            }
        }
    }

    private void writeFromTableToTopic(TopicClient topicClient, QueryClient queryClient,
                                                WriterSettings writerSettings, long id) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        retryCtx.supplyStatus(querySession -> {
            // Create new transaction object. It is not yet started on server
            QueryTransaction transaction = querySession.createNewTransaction(TxMode.SERIALIZABLE_RW);

            // Execute a query to start a new transaction
            QueryStream queryStream = transaction.createQuery(
                    "DECLARE $id AS Uint64; " +
                    "SELECT value FROM table WHERE id=$id",
                    Params.of("$id", PrimitiveValue.newUint64(id)));

            // Get query result
            QueryReader queryReader = QueryReader.readFrom(queryStream).join().getValue();
            ResultSetReader resultSet = queryReader.getResultSet(0);
            if (!resultSet.next()) {
                throw new RuntimeException("Value for id=" + id + " not found");
            }
            String value = resultSet.getColumn("value").getText();

            // Create and initialize a writer
            SyncWriter writer = topicClient.createSyncWriter(writerSettings);
            logger.info("SyncWriter created");
            writer.initAndWait();
            logger.info("SyncWriter initialized");

            // Write a message
            writer.send(
                    Message.of(value.getBytes()),
                    SendSettings.newBuilder()
                            .setTransaction(transaction)
                            .build()
            );
            logger.info("Message is sent");

            // flush to wait until the message will reach the server before committing transaction
            writer.flush();

            // Commit transaction
            CompletableFuture<Status> commitStatus = transaction.commit().thenApply(Result::getStatus);

            // Shutdown writer
            try {
                writer.shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException exception) {
                throw new RuntimeException("Shutdown not finished within " + SHUTDOWN_TIMEOUT_SECONDS +
                        " seconds");
            } catch (InterruptedException | ExecutionException exception) {
                throw new RuntimeException("Shutdown not finished due to exception: " + exception);
            }

            // Return commit status to SessionRetryContext function
            return commitStatus;
        }).join().expectSuccess("Couldn't read from table and write to topic in transaction");
    }

    public static void main(String[] args) {
        new TransactionWriteSync().doMain(args);
    }
}
