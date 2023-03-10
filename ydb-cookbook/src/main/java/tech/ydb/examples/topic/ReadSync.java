package tech.ydb.examples.topic;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 * @author Nikolay Perfilov
 */
public class ReadSync extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(ReadSync.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String topicPath = pathPrefix + "test_topic";
        String consumerName = "consumer1";

        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {

            ReaderSettings settings = ReaderSettings.newBuilder()
                    .setConsumerName(consumerName)
                    .addTopic(TopicReadSettings.newBuilder()
                            .setPath(topicPath)
                            .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                            .setMaxLag(Duration.ofMinutes(30))
                            .build())
                    .build();

            SyncReader reader = topicClient.createSyncReader(settings);

            // Init in background ?
            reader.init();

            Message message = reader.receive();

            logger.info("Message received: " + message.getData());

            message.commit()
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("exception while committing message: ", ex);
                        } else {
                            logger.info("message committed successfully");
                        }
                    })
                    .join();

            reader.shutdown();
        }
    }

    public static void main(String[] args) {
        new ReadSync().doMain(args);
    }
}
