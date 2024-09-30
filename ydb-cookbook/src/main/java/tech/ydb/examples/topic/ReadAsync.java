package tech.ydb.examples.topic;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.DecompressionException;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.PartitionSession;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.ReaderClosedEvent;
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
    private static final int MESSAGES_COUNT = 5;

    private final CompletableFuture<Void> messageReceivedFuture = new CompletableFuture<>();
    private long lastSeqNo = -1;

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {

        try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionPoolThreadCount(8)
                .build()) {

            ReaderSettings readerSettings = ReaderSettings.newBuilder()
                    .setConsumerName(CONSUMER_NAME)
                    .addTopic(TopicReadSettings.newBuilder()
                            .setPath(TOPIC_NAME)
                            .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                            .setMaxLag(Duration.ofMinutes(30))
                            .build())
                    .setMaxMemoryUsageBytes(MAX_MEMORY_USAGE_BYTES)
                    .build();

            ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
                    .setEventHandler(new Handler())
                    .build();

            AsyncReader reader = topicClient.createAsyncReader(readerSettings, handlerSettings);

            reader.init();

            messageReceivedFuture.join();

            reader.shutdown().join();
        }
    }

    private class Handler extends AbstractReadEventHandler {
        private final AtomicInteger messageCounter = new AtomicInteger(0);

        @Override
        public void onMessages(DataReceivedEvent event) {
            for (Message message : event.getMessages()) {
                StringBuilder str = new StringBuilder("Message received");
                if (logger.isTraceEnabled()) {
                    byte[] messageData;
                    try {
                        messageData = message.getData();
                    } catch (DecompressionException e) {
                        logger.warn("Decompression exception while receiving a message: ", e);
                        messageData = e.getRawData();
                    }
                    str.append(": \"").append(new String(messageData, StandardCharsets.UTF_8)).append("\"");
                }
                str.append("\n");
                if (logger.isDebugEnabled()) {
                    str.append("  offset: ").append(message.getOffset()).append("\n")
                            .append("  seqNo: ").append(message.getSeqNo()).append("\n")
                            .append("  createdAt: ").append(message.getCreatedAt()).append("\n")
                            .append("  messageGroupId: ").append(message.getMessageGroupId()).append("\n")
                            .append("  producerId: ").append(message.getProducerId()).append("\n")
                            .append("  writtenAt: ").append(message.getWrittenAt()).append("\n")
                            .append("  partitionSession: ").append(message.getPartitionSession().getId()).append("\n")
                            .append("  partitionId: ").append(message.getPartitionSession().getPartitionId())
                            .append("\n");
                    if (!message.getWriteSessionMeta().isEmpty()) {
                        str.append("  writeSessionMeta:\n");
                        message.getWriteSessionMeta().forEach((key, value) ->
                                str.append("    ").append(key).append(": ").append(value).append("\n"));
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(str.toString());
                    } else {
                        logger.debug(str.toString());
                    }
                } else {
                    logger.info("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());
                }
                if (lastSeqNo > message.getSeqNo()) {
                    logger.error("Received a message with seqNo {}. Previously got a message with seqNo {}",
                            message.getSeqNo(), lastSeqNo);
                    messageReceivedFuture.complete(null);
                } else {
                    lastSeqNo = message.getSeqNo();
                }
                message.commit().thenRun(() -> {
                    logger.info("Message committed");
                    if (messageCounter.incrementAndGet() >= MESSAGES_COUNT) {
                        logger.info("{} messages committed. Finishing reading.", MESSAGES_COUNT);
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
            logger.info("Partition session {} stopped. Committed offset: {}", event.getPartitionSessionId(),
                    event.getCommittedOffset());
            // This event means that no more messages will be received by server
            // Received messages still can be read from ReaderBuffer
            // Messages still can be committed, until confirm() method is called

            // Confirm that session can be closed
            event.confirm();
        }

        @Override
        public void onPartitionSessionClosed(PartitionSessionClosedEvent event) {
            logger.info("Partition session {} is closed.", event.getPartitionSession().getPartitionId());
        }

        @Override
        public void onReaderClosed(ReaderClosedEvent event) {
            logger.info("Reader is closed.");
            if (!messageReceivedFuture.isDone()) {
                messageReceivedFuture.completeExceptionally(
                        new RuntimeException("Reader closed before all messages are read"));
            }
        }
    }

    public static void main(String[] args) {
        new ReadAsync().doMain(args);
    }
}
