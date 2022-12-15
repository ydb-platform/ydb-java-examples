package tech.ydb.examples.topic;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.DefaultReadEventHandler;
import tech.ydb.topic.read.ReadSession;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReadSessionSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 * @author Nikolay Perfilov
 */
public class ReadAsync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(ReadAsync.class);
    private static final long MAX_MEMORY_USAGE_BYTES = 500 * 1024 * 1024; // 500 Mb

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String topicPath = pathPrefix + "test_topic";
        String consumerName = "consumer1";

        TopicClient topicClient = TopicClient.newClient(transport).build();

        ReadSessionSettings settings = ReadSessionSettings.newBuilder()
                .setConsumerName(consumerName)
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(topicPath)
                        .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                        .setMaxLag(Duration.ofMinutes(30))
                        .build())
                .setMaxMemoryUsageBytes(MAX_MEMORY_USAGE_BYTES)
                .setHandlersSettings(ReadEventHandlersSettings.newBuilder()
                        .setExecutor(ForkJoinPool.commonPool())
                        .setEventHandler(new Handler())
                        .build())
                .build();

        ReadSession readSession = topicClient.createReadSession(settings);

        readSession.start();

        readSession.waitForFinish();
    }

    private static class Handler extends DefaultReadEventHandler {

        @Override
        public void onMessages(DataReceivedEvent event) {
            for (Message message : event.getMessages()) {
                logger.info("Message received: " + message.getData());
            }
            event.commit();
        }

        @Override
        public void onStartPartitionSession(StartPartitionSessionEvent event) {
            logger.info("Partition session started." +
                    " Committed offset: " + event.getCommittedOffset() +
                    " End offset: " + event.getEndOffset());
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
        }
    }

    public static void main(String[] args) {
        new ReadAsync().doMain(args);
    }
}
