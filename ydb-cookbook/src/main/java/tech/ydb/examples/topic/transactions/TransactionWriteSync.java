package tech.ydb.examples.topic.transactions;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.MetadataItem;
import tech.ydb.topic.settings.SendSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

/**
 * @author Nikolay Perfilov
 */
public class TransactionWriteSync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(TransactionWriteSync.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String producerId = "messageGroup1";
        String messageGroupId = "messageGroup1";

        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {
            try (TableClient tableClient = TableClient.newClient(transport).build()) {
                WriterSettings settings = WriterSettings.newBuilder()
                        .setTopicPath(TOPIC_NAME)
                        .setProducerId(producerId)
                        .setMessageGroupId(messageGroupId)
                        .setCodec(Codec.ZSTD)
                        .setMaxSendBufferMessagesCount(100)
                        .build();

                SyncWriter writer = topicClient.createSyncWriter(settings);

                logger.info("SyncWriter created ");

                try {
                    writer.initAndWait();
                    logger.info("Init finished");
                } catch (Exception exception) {
                    logger.error("Exception while initializing writer: ", exception);
                    return;
                }

                long timeoutSeconds = 5; // How long should we wait for a message to be put into sending buffer
                // creating session and transaction
                Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
                if (!sessionResult.isSuccess()) {
                    logger.error("Couldn't get a session from the pool: {}", sessionResult);
                    return; // retry or shutdown
                }
                Session session = sessionResult.getValue();
                TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

                // do something else in transaction
                Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
                        .join();
                if (!dataQueryResult.isSuccess()) {
                    logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
                    return; // retry or shutdown
                }
                String messageString = dataQueryResult.getValue().getResultSet(0).getColumn(0).getText();
                try {
                    // Non-blocking call
                    writer.send(
                            Message.of(messageString.getBytes()),
                            SendSettings.newBuilder()
                                    .setTransaction(transaction)
                                    .build(),
                            timeoutSeconds,
                            TimeUnit.SECONDS
                    );
                    logger.info("Message '{}' is sent.", messageString);
                } catch (TimeoutException exception) {
                    logger.error("Send queue is full. Couldn't put message \"{}\" into sending queue within {} seconds",
                            messageString, timeoutSeconds);
                } catch (InterruptedException | ExecutionException exception) {
                    logger.error("Couldn't put message \"{}\" into sending queue due to exception: ", messageString,
                            exception);
                }

                // flush to wait until the message reach server before commit
                writer.flush();

                transaction.commit().join();

                long shutdownTimeoutSeconds = 10;
                try {
                    writer.shutdown(shutdownTimeoutSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException exception) {
                    logger.error("Shutdown was not finished within {} seconds: ", timeoutSeconds, exception);
                } catch (InterruptedException | ExecutionException exception) {
                    logger.error("Shutdown was not finished due to exception: ", exception);
                }
            }
        }
    }

    public static void main(String[] args) {
        new TransactionWriteSync().doMain(args);
    }
}
