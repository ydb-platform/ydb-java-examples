package tech.ydb.examples.topic;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.WriteAck;
import tech.ydb.topic.write.Writer;

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

        WriterSettings settings = WriterSettings.newBuilder()
                .setTopicPath(topicPath)
                .setProducerId(producerId)
                .setMessageGroupId(messageGroupId)
                .build();

        Writer writer = topicClient.createWriter(settings);

        // Init in background
        writer.start();

        // Blocks if in-flight or memory usage limit is reached
        writer.send("message1".getBytes());

        // Blocks if in-flight or memory usage limit is reached
        writer.send(Message.of("message2".getBytes()));

        // Blocks if in-flight or memory usage limit is reached
        writer.send(Message.newBuilder()
                        .setData("message3".getBytes())
                        .setSeqNo(3)
                        .setCreateTimestamp(Instant.now())
                .build());

        try {
            // Blocks if in-flight or memory usage limit is reached, but no longer than for 1 second
            writer.send("message4".getBytes(), Duration.ofSeconds(1));

            logger.info("Message 4 sent");
        } catch (TimeoutException timeout) {
            logger.error("Timeout sending message 4");
        }

        try {
            // Blocks if in-flight or memory usage limit is reached, but no longer than for 2 seconds
            writer.send(Message.newBuilder()
                        .setData("message5".getBytes())
                        .setSeqNo(5)
                        .setCreateTimestamp(Instant.now())
                    .build(),
                    Duration.ofSeconds(2));

            logger.info("Message 5 sent, confirmation received");
        } catch (TimeoutException timeout) {
            logger.error("Timeout sending message 5");
        }

        try {
            WriteAck ack = writer.sendWithAck(Message.newBuilder()
                        .setData("message6".getBytes()).setSeqNo(6)
                        .setCreateTimestamp(Instant.now())
                    .build(),
                    Duration.ofSeconds(2));

            switch (ack.getState()) {
                case WRITTEN:
                    WriteAck.Details details = ack.getDetails();
                    logger.info("Message 6 was written successfully."
                            + " PartitionId: " + details.getPartitionId()
                            + ", offset: " + details.getOffset());
                    break;
                case DISCARDED:
                    logger.warn("Message 6 was discarded");
                    break;
                case ALREADY_WRITTEN:
                    logger.warn("Message 6 was already written");
                    break;
                default:
                    break;
            }
        } catch (TimeoutException timeout) {
            logger.error("Timeout sending message 6");
        }

        try {
            writer.newMessage()
                    .setData("message7".getBytes())
                    .setSeqNo(7)
                    .setBlockingTimeout(Duration.ofSeconds(4))
                    .send();
        } catch (TimeoutException timeout) {
            logger.error("Timeout sending message 7");
        }

        writer.close();
        writer.waitForFinish();
    }

    public static void main(String[] args) {
        new WriteSync().doMain(args);
    }
}
