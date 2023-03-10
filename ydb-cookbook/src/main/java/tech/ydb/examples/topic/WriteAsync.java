package tech.ydb.examples.topic;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.QueueOverflowException;
import tech.ydb.topic.write.WriteAck;

/**
 * @author Nikolay Perfilov
 */
public class WriteAsync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(WriteAsync.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String topicPath = pathPrefix + "topic-java";
        String producerId = "messageGroup1";
        String messageGroupId = "messageGroup1";

        ExecutorService compressionExecutor = Executors.newFixedThreadPool(10);
        try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(compressionExecutor)
                .build()) {

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

            for (int i = 1; i <= 5; i++) {
                final int index = i;
                try {
                    String messageString = "message" + i;
                    // Blocks until the message is put into sending buffer
                    writer.send(Message.of(messageString.getBytes())).whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("Exception while sending message {}: ", index, ex);
                        } else {
                            logger.info("Message {} ack received", index);

                            switch (result.getState()) {
                                case WRITTEN:
                                    WriteAck.Details details = result.getDetails();
                                    logger.info("Message was written successfully."
                                            + ", offset: " + details.getOffset());
                                    break;
                                case ALREADY_WRITTEN:
                                    logger.warn("Message was already written");
                                    break;
                                default:
                                    break;
                            }
                        }
                    });
                } catch (QueueOverflowException exception) {
                    logger.error("Queue overflow exception while sending message{}: ", index, exception);
                    // Send queue is full. Need retry with backoff or skip
                }

                logger.info("Message {} is sent", index);
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
        compressionExecutor.shutdown();
    }

    public static void main(String[] args) {
        new WriteAsync().doMain(args);
    }
}
