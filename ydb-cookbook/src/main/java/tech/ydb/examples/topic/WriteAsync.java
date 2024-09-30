package tech.ydb.examples.topic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private static final int MESSAGES_COUNT = 5;
    private static final int WAIT_TIMEOUT_SECONDS = 60;

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String producerId = "messageGroup1";
        String messageGroupId = "messageGroup1";

        ExecutorService compressionExecutor = Executors.newFixedThreadPool(10);
        try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(compressionExecutor)
                .build()) {

            WriterSettings settings = WriterSettings.newBuilder()
                    .setTopicPath(TOPIC_NAME)
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

            // A latch to wait for all writes to receive a WriteAck before shutting down writer
            CountDownLatch writesInProgress = new CountDownLatch(MESSAGES_COUNT);

            for (int i = 1; i <= MESSAGES_COUNT; i++) {
                final int index = i;
                try {
                    String messageString = "message" + i;
                    // Blocks until the message is put into sending buffer
                    writer.send(Message.of(messageString.getBytes())).whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("Exception while sending a message {}: ", index, ex);
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
                        writesInProgress.countDown();
                    });
                } catch (QueueOverflowException exception) {
                    logger.error("Queue overflow exception while sending a message{}: ", index, exception);
                    // Send queue is full. Need to retry with backoff or skip
                    writesInProgress.countDown();
                }

                logger.info("Message {} is sent", index);
            }

            try {
                while (!writesInProgress.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    logger.error("Writes are not finished in {} seconds", WAIT_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException exception) {
                logger.error("Waiting for writes to finish was interrupted: ", exception);
            }

            try {
                if (!writesInProgress.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    logger.error("Writes are not finished in {} seconds", WAIT_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException exception) {
                logger.error("Waiting for writes to finish was interrupted: ", exception);
            }

            try {
                writer.shutdown().get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException exception) {
                logger.error("Timeout exception during writer termination ({} seconds): ", WAIT_TIMEOUT_SECONDS,
                        exception);
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
