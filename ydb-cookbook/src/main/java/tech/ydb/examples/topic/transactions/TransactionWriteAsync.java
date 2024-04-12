package tech.ydb.examples.topic.transactions;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.settings.SendSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.QueueOverflowException;
import tech.ydb.topic.write.WriteAck;

/**
 * @author Nikolay Perfilov
 */
public class TransactionWriteAsync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(TransactionWriteAsync.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String topicPath = pathPrefix + "topic-java";
        String producerId = "messageGroup1";
        String messageGroupId = "messageGroup1";

        ExecutorService compressionExecutor = Executors.newFixedThreadPool(10);
        try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(compressionExecutor)
                .build()) {
            try (TableClient tableClient = TableClient.newClient(transport).build()) {

                WriterSettings settings = WriterSettings.newBuilder()
                        .setTopicPath(topicPath)
                        .setProducerId(producerId)
                        .setMessageGroupId(messageGroupId)
                        .setCodec(Codec.GZIP)
                        .setMaxSendBufferMemorySize(50 * 1024 * 1024)
                        .build();

                AsyncWriter writer = topicClient.createAsyncWriter(settings);

                // Init in background
                writer.init()
                        .thenRun(() -> logger.info("Init finished successfully"))
                        .exceptionally(ex -> {
                            logger.error("Init failed with ex: ", ex);
                            return null;
                        });

                // creating session and transaction
                Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
                if (!sessionResult.isSuccess()) {
                    logger.error("Couldn't get a session from the pool: {}", sessionResult);
                    return; // retry or shutdown
                }
                Session session = sessionResult.getValue();
                TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

                // get message text within the transaction
                Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
                        .join();
                if (!dataQueryResult.isSuccess()) {
                    logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
                    return; // retry or shutdown
                }
                ResultSetReader rsReader = dataQueryResult.getValue().getResultSet(0);
                byte[] message;
                if (rsReader.next()) {
                    message = rsReader.getColumn(0).getBytes();
                } else {
                    logger.error("Empty DataQuery result");
                    return; // retry or shutdown
                }

                try {
                    // Blocks until the message is put into sending buffer
                    writer.send(Message.newBuilder()
                                            .setData(message)
                                            .build(),
                                    SendSettings.newBuilder()
                                            .setTransaction(transaction)
                                            .build())
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    logger.error("Exception while sending a message: ", ex);
                                } else {
                                    logger.info("Message ack received");

                                    switch (result.getState()) {
                                        case WRITTEN:
                                            WriteAck.Details details = result.getDetails();
                                            logger.info("Message was written successfully, offset: " +
                                                    details.getOffset());
                                            break;
                                        case ALREADY_WRITTEN:
                                            logger.warn("Message was already written");
                                            break;
                                        default:
                                            break;
                                    }
                                }
                            })
                            // Waiting for the message to reach the server before committing the transaction
                            .join();

                    logger.info("Message is sent");

                    transaction.commit().whenComplete((status, throwable) -> {
                        if (throwable != null) {
                            logger.error("Exception while committing transaction with message: ", throwable);
                        } else {
                            if (status.isSuccess()) {
                                logger.info("Transaction with message committed successfully");
                            } else {
                                logger.error("Failed to commit transaction with message: {}", status);
                            }
                        }
                    });
                } catch (QueueOverflowException exception) {
                    logger.error("Queue overflow exception while sending a message: ", exception);
                    // Send queue is full. Need to retry with backoff or skip
                }

                long timeoutSeconds = 10;
                try {
                    writer.shutdown().get(timeoutSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException exception) {
                    logger.error("Timeout exception during writer termination ({} seconds): ", timeoutSeconds, exception);
                } catch (ExecutionException exception) {
                    logger.error("Execution exception during writer termination: ", exception);
                } catch (InterruptedException exception) {
                    logger.error("Writer termination was interrupted: ", exception);
                }
            }
        }
        compressionExecutor.shutdown();
    }

    public static void main(String[] args) {
        new TransactionWriteAsync().doMain(args);
    }
}
