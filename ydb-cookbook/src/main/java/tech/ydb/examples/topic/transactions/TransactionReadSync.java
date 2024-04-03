package tech.ydb.examples.topic.transactions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.DecompressionException;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.ReceiveSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 * @author Nikolay Perfilov
 */
public class TransactionReadSync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(TransactionReadSync.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {

        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {
            try (TableClient tableClient = TableClient.newClient(transport).build()) {
                ReaderSettings settings = ReaderSettings.newBuilder()
                        .setConsumerName(CONSUMER_NAME)
                        .addTopic(TopicReadSettings.newBuilder()
                                .setPath(TOPIC_NAME)
                                .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                                .setMaxLag(Duration.ofMinutes(30))
                                .build())
                        .build();

                SyncReader reader = topicClient.createSyncReader(settings);

                // Init in background
                reader.init();

                try {
                    // Reading 5 messages
                    for (int i = 0; i < 5; i++) {
                        // creating session and transaction
                        Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
                        if (!sessionResult.isSuccess()) {
                            logger.error("Couldn't a get session from the pool: {}", sessionResult);
                            return; // retry or shutdown
                        }
                        Session session = sessionResult.getValue();
                        TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

                        // do something else in transaction
                        transaction.executeDataQuery("SELECT 1").join();
                        // analyzeQueryResultIfNeeded();

                        //Session session
                        Message message = reader.receive(ReceiveSettings.newBuilder()
                                .setTransaction(transaction)
                                .build());
                        byte[] messageData;
                        try {
                            messageData = message.getData();
                        } catch (DecompressionException e) {
                            logger.warn("Decompression exception while receiving a message: ", e);
                            messageData = e.getRawData();
                        }
                        logger.info("Message received: {}", new String(messageData, StandardCharsets.UTF_8));

                        transaction.commit().join();
                        // analyze commit status
                    }
                } catch (InterruptedException exception) {
                    logger.error("Interrupted exception while waiting for message: ", exception);
                }

                reader.shutdown();
            }
        }
    }

    public static void main(String[] args) {
        new TransactionReadSync().doMain(args);
    }
}
