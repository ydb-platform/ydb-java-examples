package tech.ydb.examples.topic;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.WriteSessionSettings;
import tech.ydb.topic.settings.WriteSettings;
import tech.ydb.topic.write.WriteAck;
import tech.ydb.topic.write.WriteSession;

/**
 * @author Nikolay Perfilov
 */
public class WriteSync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(WriteSync.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String topicPath = pathPrefix + "test_topic";
        String producerId = "producer1";
        String messageGroupId = "mg1";

        TopicClient topicClient = TopicClient.newClient(transport).build();

        WriteSessionSettings settings = WriteSessionSettings.newBuilder()
                .setTopicPath(topicPath)
                .setProducerId(producerId)
                .setMessageGroupId(messageGroupId)
                .build();

        WriteSession writeSession = topicClient.createWriteSession(settings);

        // Init in background
        writeSession.start();

        // Blocking call
        writeSession.send("message1".getBytes());

        try {
            // Blocking call
            writeSession.send("message2".getBytes(), WriteSettings.newBuilder()
                    .setTimeout(Duration.ofSeconds(10))
                    .setCreateTimestamp(Instant.now())
                    .setSeqNo(2)
                    .setBlockingTimeout(Duration.ofSeconds(5))
                    .build());

            logger.info("Message 2 sent, confirmation received");
        } catch (TimeoutException timeout) {
            logger.error("Timeout sending message 2");
        }

        try {
            WriteAck ack = writeSession.newMessage()
                    .setData("message3".getBytes())
                    .setTimeout(Duration.ofSeconds(1))
                    .setSeqNo(3)
                    .send();

            switch (ack.getState()) {
                case WRITTEN:
                    WriteAck.Details details = ack.getDetails();
                    logger.info("Message 3 was written successfully."
                            + " PartitionId: " + details.getPartitionId()
                            + ", offset: " + details.getOffset());
                    break;
                case DISCARDED:
                    logger.warn("Message 3 was discarded");
                    break;
                case ALREADY_WRITTEN:
                    logger.warn("Message 3 was already written");
                    break;
                default:
                    break;
            }
        } catch (TimeoutException timeout) {
            logger.error("Timeout sending message 3");
        }

        writeSession.close();
        writeSession.waitForFinish();
    }

    public static void main(String[] args) {
        new WriteSync().doMain(args);
    }
}
