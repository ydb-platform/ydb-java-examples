package tech.ydb.examples.topic;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

/**
 * @author Nikolay Perfilov
 */
public class WriteSync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(WriteSync.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String producerId = "messageGroup1";
        String messageGroupId = "messageGroup1";

        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {

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

            for (int i = 1; i <= 5; i++) {
                try {
                    String messageString = "message" + i;
                    // Non-blocking call
                    writer.send(
                            Message.of(messageString.getBytes()),
                            timeoutSeconds,
                            TimeUnit.SECONDS
                    );
                    logger.info("Message '{}' is sent.", messageString);
                } catch (TimeoutException exception) {
                    logger.error("Send queue is full. Couldn't put message {} into sending queue within {} seconds",
                            i, timeoutSeconds);
                } catch (InterruptedException | ExecutionException exception) {
                    logger.error("Couldn't put message {} into sending queue due to exception: ", i, exception);
                }
            }

            writer.flush();
            logger.info("Flush finished");
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

    public static void main(String[] args) {
        new WriteSync().doMain(args);
    }
}
