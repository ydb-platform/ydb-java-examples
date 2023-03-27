package tech.ydb.examples.topic;

import java.nio.charset.StandardCharsets;
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
        String topicPath = pathPrefix + "topic-java";
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

            // Init in background
            reader.init();


            try {
                // Reading 5 messages
                for (int i = 0; i < 5; i++) {
                    Message message = reader.receive();
                    logger.info("Message received: {}", new String(message.getData(), StandardCharsets.UTF_8));

                    message.commit()
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    logger.error("exception while committing message: ", ex);
                                } else {
                                    logger.info("message committed successfully");
                                }
                            })
                            // Usually it is a bad idea to block on message commit. Doing this to simplify the example
                            .join();
                }
            } catch (InterruptedException exception) {
                logger.error("Interrupted exception while waiting for message: ", exception);
            }

            reader.shutdown();
        }
    }

    public static void main(String[] args) {
        new ReadSync().doMain(args);
    }
}
