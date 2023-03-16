package tech.ydb.examples.topic;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.PartitionSession;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 * @author Nikolay Perfilov
 */
public class ReadAsync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(ReadAsync.class);
    private static final long MAX_MEMORY_USAGE_BYTES = 500 * 1024 * 1024; // 500 Mb

    private static final CompletableFuture<Void> messageReceivedFuture = new CompletableFuture<>();

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String topicPath = pathPrefix + "topic-java";
        String consumerName = "consumer1";

        try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionPoolThreadCount(8)
                .build()) {

            ReaderSettings readerSettings = ReaderSettings.newBuilder()
                    .setConsumerName(consumerName)
                    .addTopic(TopicReadSettings.newBuilder()
                            .setPath(topicPath)
                            .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                            .setMaxLag(Duration.ofMinutes(30))
                            .build())
                    .setMaxMemoryUsageBytes(MAX_MEMORY_USAGE_BYTES)
                    .build();

            ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
                    //.setExecutor(ForkJoinPool.commonPool())
                    .setEventHandler(new Handler())
                    .build();

            AsyncReader reader = topicClient.createAsyncReader(readerSettings, handlerSettings);

            reader.init();

            messageReceivedFuture.join();

            reader.shutdown().join();
        }
    }

    private static class Handler extends AbstractReadEventHandler {
        private final AtomicInteger messageCounter = new AtomicInteger(0);

        @Override
        public void onMessages(DataReceivedEvent event) {
            for (Message message : event.getMessages()) {
                StringBuilder str = new StringBuilder();
                str.append("Message received: \"").append(new String(message.getData(), StandardCharsets.UTF_8))
                        .append("\"\n")
                        .append("  offset: ").append(message.getOffset()).append("\n")
                        .append("  seqNo: ").append(message.getSeqNo()).append("\n")
                        .append("  createdAt: ").append(message.getCreatedAt()).append("\n")
                        .append("  messageGroupId: ").append(message.getMessageGroupId()).append("\n")
                        .append("  producerId: ").append(message.getProducerId()).append("\n")
                        .append("  writtenAt: ").append(message.getWrittenAt()).append("\n");
                if (!message.getWriteSessionMeta().isEmpty()) {
                    str.append("  writeSessionMeta:\n");
                    message.getWriteSessionMeta().forEach((key, value) ->
                            str.append("    ").append(key).append(": ").append(value).append("\n"));
                }
                logger.info(str.toString());
                message.commit().thenRun(() -> {
                    logger.info("Message committed");
                    if (messageCounter.incrementAndGet() >= 5) {
                        logger.info("5 messages committed. Finishing reading.");
                        messageReceivedFuture.complete(null);
                    }
                });
            }
        }

        @Override
        public void onStartPartitionSession(StartPartitionSessionEvent event) {
            StringBuilder str = new StringBuilder();
            PartitionSession partitionSession = event.getPartitionSession();
            str.append("Partition session started.\n")
                    .append("  Partition session Id: ").append(partitionSession.getId()).append("\n")
                    .append("  Partition Id: ").append(partitionSession.getPartitionId()).append("\n")
                    .append("  Path: ").append(partitionSession.getPath()).append("\n")
                    .append("  Committed offset: ").append(event.getCommittedOffset()).append("\n")
                    .append("  Partition offsets: [").append(event.getPartitionOffsets().getStart()).append(", ")
                    .append(event.getPartitionOffsets().getEnd()).append(")");
            logger.info(str.toString());
            event.confirm();
        }

        @Override
        public void onStopPartitionSession(StopPartitionSessionEvent event) {
            logger.info("Partition session stopped." +
                    " Committed offset: " + event.getCommittedOffset());
            event.confirm();
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("Error occurred while reading: " + throwable);
            messageReceivedFuture.completeExceptionally(throwable);
        }
    }

    public static void main(String[] args) {
        new ReadAsync().doMain(args);
    }
}
