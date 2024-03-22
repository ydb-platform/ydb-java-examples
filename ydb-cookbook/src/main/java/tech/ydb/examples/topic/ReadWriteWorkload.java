package tech.ydb.examples.topic;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.PartitionSession;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.ReaderClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

/**
 * @author Nikolay Perfilov
 */
public class ReadWriteWorkload extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(ReadWriteWorkload.class);
    private static final int WRITE_TIMEOUT_SECONDS = 60;
    private static final int MESSAGE_LENGTH_BYTES = 10_000_000; // 10 Mb
    private static final int WRITE_BUFFER_SIZE_BYTES = 50_000_000; // 50 Mb
    private static final long MAX_READER_MEMORY_USAGE_BYTES = 500_000_000; // 500 Mb
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 60;
    private final AtomicInteger unreadMessagesCount = new AtomicInteger(0);

    private final AtomicInteger messagesSent = new AtomicInteger(0);
    private final AtomicInteger messagesReceived = new AtomicInteger(0);
    private final AtomicInteger messagesCommitted = new AtomicInteger(0);
    private final AtomicLong bytesWritten = new AtomicLong(0);
    private long lastSeqNo = -1;
    CountDownLatch writeFinishedLatch = new CountDownLatch(1);
    CountDownLatch readFinishedLatch = new CountDownLatch(1);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {

        ExecutorService compressionExecutor = Executors.newFixedThreadPool(10);
        AtomicBoolean timeToStopWriting = new AtomicBoolean(false);
        try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(compressionExecutor)
                .build()) {
            final Instant startTime = Instant.now();

            Runnable writingThread = () -> {
                String producerId = "messageGroup1";
                String messageGroupId = "messageGroup1";
                WriterSettings settings = WriterSettings.newBuilder()
                        .setTopicPath(TOPIC_NAME)
                        .setProducerId(producerId)
                        .setMessageGroupId(messageGroupId)
                        .setCodec(Codec.GZIP)
                        .setMaxSendBufferMemorySize(WRITE_BUFFER_SIZE_BYTES)
                        .build();

                SyncWriter writer = topicClient.createSyncWriter(settings);

                // Init in background
                writer.initAndWait();
                logger.info("Sync writer initialized");

                int i = 0;
                while (!timeToStopWriting.get()) {
                    final int index = i;
                    byte[] messageData = new byte[MESSAGE_LENGTH_BYTES];
                    new Random().nextBytes(messageData);
                    try {
                        // Blocks until the message is put into sending buffer
                        writer.send(Message.of(messageData), WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        messagesSent.incrementAndGet();
                        bytesWritten.addAndGet(MESSAGE_LENGTH_BYTES);
                        unreadMessagesCount.incrementAndGet();
                        logger.debug("Message {} is sent", index);
                    } catch (TimeoutException exception) {
                        logger.error("Timeout exception on writing a message ({} seconds)", WRITE_TIMEOUT_SECONDS);
                        break;
                    } catch (ExecutionException exception) {
                        logger.error("Execution exception on writing a message: ", exception);
                        break;
                    } catch (InterruptedException exception) {
                        logger.error("Interrupted exception on writing a message: ", exception);
                        break;
                    }
                    i++;
                }
                logger.info("Received a signal to stop writing");

                try {
                    writer.shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (TimeoutException exception) {
                    logger.error("Timeout exception during writer termination ({} seconds)", SHUTDOWN_TIMEOUT_SECONDS);
                } catch (ExecutionException exception) {
                    logger.error("Execution exception during writer termination: ", exception);
                } catch (InterruptedException exception) {
                    logger.error("Writer termination was interrupted: ", exception);
                }

                writeFinishedLatch.countDown();
                logger.info("Writing thread finished");
            };

            Runnable measuringThread = () -> {
                Instant lastCheck = startTime;
                long lastTimeBytesWritten = 0;
                try {
                    while (!writeFinishedLatch.await(1, TimeUnit.SECONDS)) {
                        long bytesWrittenTotal = bytesWritten.get();
                        long bytesWrittenSinceLastTime = bytesWrittenTotal - lastTimeBytesWritten;
                        Instant now = Instant.now();
                        long msPassedTotal = Duration.between(startTime, now).toMillis();
                        double writingSpeedTotal = msPassedTotal > 0
                                ? (double)bytesWrittenTotal / 1000 / msPassedTotal : 0;
                        long msPassedRecent = Duration.between(lastCheck, now).toMillis();
                        double writingSpeedRecent = msPassedRecent > 0
                                ? (double)bytesWrittenSinceLastTime / 1000 / msPassedRecent : 0;
                        logger.info("Recent writing speed: {} Mb/s. Total writing speed: {} Mb/s",
                                doubleToStr(writingSpeedRecent, 1),
                                doubleToStr(writingSpeedTotal, 1));
                        lastCheck = now;
                        lastTimeBytesWritten = bytesWrittenTotal;
                    }
                } catch (InterruptedException exception) {
                    logger.error("Waiting in measuring thread was interrupted: ", exception);
                }
                logger.info("Measuring thread finished");
            };

            Runnable readingThread = () -> {
                String consumerName = "consumer1";
                ReaderSettings readerSettings = ReaderSettings.newBuilder()
                        .setConsumerName(consumerName)
                        .addTopic(TopicReadSettings.newBuilder()
                                .setPath(TOPIC_NAME)
                                .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                                .setMaxLag(Duration.ofMinutes(30))
                                .build())
                        .setMaxMemoryUsageBytes(MAX_READER_MEMORY_USAGE_BYTES)
                        .build();

                ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
                        .setEventHandler(new Handler())
                        .build();

                AsyncReader reader = topicClient.createAsyncReader(readerSettings, handlerSettings);

                reader.init();
                logger.info("Reader initialized");

                try {
                    writeFinishedLatch.await();
                    logger.info("Reading thread: writing was finished. Waiting for reading to finish...");
                } catch (InterruptedException exception) {
                    logger.error("Waiting for writing to finish was interrupted");
                }

                try {
                    if (unreadMessagesCount.get() <= 0
                            || readFinishedLatch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        logger.info("All messages are read");
                    } else {
                        logger.error("Reading was not finished after {} seconds ", SHUTDOWN_TIMEOUT_SECONDS);
                    }
                } catch (InterruptedException exception) {
                    logger.error("Waiting for reading to finish was interrupted");
                }

                reader.shutdown().join();
                logger.info("Reading thread finished");
            };
            ExecutorService taskExecutor = Executors.newFixedThreadPool(3);
            taskExecutor.submit(writingThread);
            taskExecutor.submit(measuringThread);
            taskExecutor.submit(readingThread);

            // Waiting for any input
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            scanner.close();
            // Signalling writing stream to shutdown
            timeToStopWriting.set(true);

            Instant now = Instant.now();
            long bytesWrittenTotal = bytesWritten.get();
            Duration timePassedTotal = Duration.between(startTime, now);
            long msPassedTotal = timePassedTotal.toMillis();
            double writingSpeedTotal = msPassedTotal > 0 ? (double)bytesWrittenTotal / 1000 / msPassedTotal : 0;
            logger.info("Has written {} bytes in {} seconds.\nTotal writing speed: {} Mb/s.", bytesWrittenTotal,
                    timePassedTotal.getSeconds(), doubleToStr(writingSpeedTotal, 1));

            taskExecutor.shutdown();
            try {
                if (taskExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    logger.info("All tasks completed successfully");
                } else {
                    logger.error("Tasks were not finished in {} seconds", SHUTDOWN_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException exception) {
                logger.error("Exception while waiting for tasks termination: ", exception);
            }
        }
        compressionExecutor.shutdown();

        logger.info("messagesSent: {}\nmessagesReceived: {}\nmessagesCommitted: {}", messagesSent, messagesReceived,
                messagesCommitted);
    }

    private static String doubleToStr(double value, int maximumFractionDigits) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(maximumFractionDigits);
        return df.format(value);
    }

    private class Handler extends AbstractReadEventHandler {

        @Override
        public void onMessages(DataReceivedEvent event) {
            for (tech.ydb.topic.read.Message message : event.getMessages()) {
                messagesReceived.incrementAndGet();
                if (logger.isTraceEnabled()) {
                    StringBuilder str = new StringBuilder("Message received");
                    str.append("\n");
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
                    logger.trace(str.toString());
                } else {
                    logger.debug("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());
                }
                if (lastSeqNo > message.getSeqNo()) {
                    logger.error("Received a message with seqNo {}. Previously got a message with seqNo {}",
                            message.getSeqNo(), lastSeqNo);
                } else {
                    lastSeqNo = message.getSeqNo();
                }
                message.commit().thenRun(() -> {
                    logger.trace("Message committed");
                    unreadMessagesCount.decrementAndGet();
                    messagesCommitted.incrementAndGet();
                    if (writeFinishedLatch.getCount() == 0 && unreadMessagesCount.get() == 0) {
                        logger.info("All messages were read and committed. Finishing reading.");
                        readFinishedLatch.countDown();
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
        }
    }

    public static void main(String[] args) {
        new ReadWriteWorkload().doMain(args);
    }
}
