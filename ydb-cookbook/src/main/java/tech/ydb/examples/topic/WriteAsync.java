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
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.WriteAck;
import tech.ydb.topic.write.Writer;

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

        WriterSettings settings = WriterSettings.newBuilder()
                .setTopicPath(topicPath)
                .setProducerId(producerId)
                .setMessageGroupId(messageGroupId)
                .setBatchFlushInterval(Duration.ofSeconds(1))
                .setBatchFlushSiseBytes(2 * 1024 * 1024)
                .setCodec(Codec.ZSTD)
                .setCompressionLevel(3)
                .setMaxMemoryUsageBytes(50 * 1024 * 1024)
                .build();

        Writer writer = topicClient.createWriter(settings);

        // Init in background
        writer.start();

        // Non-blocking call
        CompletableFuture<WriteAck> future1 = writer.sendAsync("message1".getBytes());

        logger.info("Message 1 sent");


        CompletableFuture<WriteAck> future2 = writer.sendAsync(Message.newBuilder()
                        .setData("message2".getBytes())
                        .setCreateTimestamp(Instant.now())
                        .setSeqNo(2)
                        .build()
                , Duration.ofSeconds(5));

        logger.info("Message 2 sent");

        CompletableFuture<WriteAck> future3 = writer.newMessage()
                .setData("message3".getBytes())
                .setSeqNo(3)
                .setBlockingTimeout(Duration.ofSeconds(1))
                .sendAsync();

        CompletableFuture.allOf(future1, future2, future3);

        for (CompletableFuture<WriteAck> future : Arrays.asList(future1, future2, future3)) {
            try {
                WriteAck ack = future.join();

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
            } catch (Exception exception) {
                logger.error("Exception while sending a message: " + exception);
            }
        }

        writer.close();
        writer.waitForFinish();
    }

    public static void main(String[] args) {
        new WriteAsync().doMain(args);
    }
}
