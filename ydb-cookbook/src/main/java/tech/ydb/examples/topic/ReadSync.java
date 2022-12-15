package tech.ydb.examples.topic;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.ReadSession;
import tech.ydb.topic.settings.ReadSessionSettings;
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

        TopicClient topicClient = TopicClient.newClient(transport).build();

        ReadSessionSettings settings = ReadSessionSettings.newBuilder()
                .setConsumerName(consumerName)
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(topicPath)
                        .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                        .setMaxLag(Duration.ofMinutes(30))
                        .build())
                .build();

        ReadSession readSession = topicClient.createReadSession(settings);

        // Init in background ?
        readSession.start();

        Message message = readSession.receive();

        logger.info("Message received: " + message.getData());

        message.commit();

        readSession.close();

    }

    public static void main(String[] args) {
        new ReadSync().doMain(args);
    }
}
