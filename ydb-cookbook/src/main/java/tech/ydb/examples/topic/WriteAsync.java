package tech.ydb.examples.topic;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.settings.WriteSessionSettings;
import tech.ydb.topic.settings.WriteSettings;
import tech.ydb.topic.write.WriteAck;
import tech.ydb.topic.write.WriteSession;

/**
 * @author Nikolay Perfilov
 */
public class WriteAsync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(WriteAsync.class);

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
                .setBatchFlushInterval(Duration.ofSeconds(1))
                .setBatchFlushSiseBytes(2 * 1024 * 1024)
                .setCodec(Codec.ZSTD)
                .setCompressionLevel(3)
                .setMaxMemoryUsageBytes(50 * 1024 * 1024)
                .build();

        WriteSession writeSession = topicClient.createWriteSession(settings);

        // Init in background
        writeSession.start();

        // Non-blocking call
        CompletableFuture<WriteAck> future1 = writeSession.sendAsync("message1".getBytes());

        logger.info("Message 1 sent");

        // Non-blocking call
        CompletableFuture<WriteAck> future2 = writeSession.sendAsync("message2".getBytes(), WriteSettings.newBuilder()
                .setTimeout(Duration.ofSeconds(10))
                .setCreateTimestamp(Instant.now())
                .setSeqNo(2)
                .setBlockingTimeout(Duration.ofSeconds(5))
                .build());

        logger.info("Message 2 sent");

        CompletableFuture<WriteAck> future3 = writeSession.newMessage()
                .setData("message3".getBytes())
                .setTimeout(Duration.ofSeconds(1))
                .setSeqNo(3)
                .sendAsync();

        CompletableFuture.allOf(future1, future2, future3);

        for (CompletableFuture<WriteAck> future : Arrays.asList(future1, future2, future3)) {
            try {
                WriteAck ack = future.get();

                switch (ack.getState()) {
                    case WRITTEN:
                        WriteAck.Details details = ack.getDetails();
                        logger.info("Message was written successfully."
                                + " PartitionId: " + details.getPartitionId()
                                + ", offset: " + details.getOffset());
                        break;
                    case DISCARDED:
                        logger.warn("Message was discarded");
                        break;
                    case ALREADY_WRITTEN:
                        logger.warn("Message was already written");
                        break;
                    default:
                        break;
                }
            } catch (InterruptedException exception) {
                logger.error("Message sending was interrupted");
            } catch (Exception exception) {
                logger.error("Exception while sending a message: " + exception);
            }
        }

        writeSession.close();
        writeSession.waitForFinish();
    }

    public static void main(String[] args) {
        new WriteAsync().doMain(args);
    }
}
