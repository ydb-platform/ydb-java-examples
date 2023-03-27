package tech.ydb.examples.topic;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.SimpleExample;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.description.SupportedCodecs;
import tech.ydb.topic.description.TopicDescription;
import tech.ydb.topic.settings.AlterConsumerSettings;
import tech.ydb.topic.settings.AlterPartitioningSettings;
import tech.ydb.topic.settings.AlterTopicSettings;
import tech.ydb.topic.settings.CreateTopicSettings;
import tech.ydb.topic.settings.DropTopicSettings;
import tech.ydb.topic.settings.PartitioningSettings;

/**
 * @author Nikolay Perfilov
 */
public class ControlPlane extends SimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(ControlPlane.class);

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        logger.info("ControlPlane run");
        String topicPath = "test_topic";
        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {

            {
                // Create topic
                Map<String, String> attrs = new HashMap<>();
                attrs.put("attrName1", "attrValue1");
                attrs.put("attrName2", "attrValue2");

                List<Consumer> consumers = new ArrayList<>();
                consumers.add(Consumer.newBuilder().setName("testConsumer1").build());

                topicClient.createTopic(topicPath, CreateTopicSettings.newBuilder()
                                .setSupportedCodecs(SupportedCodecs.newBuilder()
                                        .addCodec(Codec.LZOP)
                                        .addCodec(Codec.GZIP)
                                        .build())
                                .setPartitioningSettings(new PartitioningSettings(4, 10))
                                .setPartitionWriteSpeedBytesPerSecond(2 * 1024 * 1024) // 2 MB
                                .setPartitionWriteBurstBytes(4 * 1024 * 1024) // 4 MB
                                .setConsumers(consumers)
                                .addConsumer(Consumer.newBuilder()
                                        .setName("testConsumer2")
                                        .setAttributes(attrs)
                                        .addAttribute("attrName3", "attrValue3")
                                        .setImportant(false)
                                        .setReadFrom(Instant.now().minus(Duration.ofHours(10)))
                                        .build())
                                .withOperationTimeout(Duration.ofSeconds(3))
                                .withRequestTimeout(Duration.ofSeconds(4))
                                .build())
                        .join()
                        .expectSuccess("cannot create topic: " + topicPath);
            }

            {
                // Describe topic after creation
                TopicDescription description = describeTopic(topicPath, topicClient);

                assert description.getPartitioningSettings().getMinActivePartitions() == 4;
                assert description.getPartitioningSettings().getPartitionCountLimit() == 10;
                assert description.getPartitionWriteSpeedBytesPerSecond() == 2 * 1024 * 1024;
                assert description.getPartitionWriteBurstBytes() == 4 * 1024 * 1024;
                assert description.getConsumers().size() == 2;
                assert description.getSupportedCodecs().getCodecs().size() == 2;
                assert description.getSupportedCodecs().getCodecs().contains(Codec.LZOP);
                assert description.getSupportedCodecs().getCodecs().contains(Codec.GZIP);
            }

            {
                // Alter topic 1
                topicClient.alterTopic(topicPath, AlterTopicSettings.newBuilder()
                                .setAlterPartitioningSettings(AlterPartitioningSettings.newBuilder()
                                        .setMinActivePartitions(6)
                                        .build())
                                .setPartitionWriteSpeedBytesPerSecond(4 * 1024 * 1024)
                                .build())
                        .join()
                        .expectSuccess("cannot alter topic: " + topicPath);
            }

            {
                // Describe topic after first alter
                TopicDescription description = describeTopic(topicPath, topicClient);

                assert description.getPartitioningSettings().getMinActivePartitions() == 6;
                assert description.getPartitioningSettings().getPartitionCountLimit() == 10;
                assert description.getPartitionWriteSpeedBytesPerSecond() == 42 * 1024 * 1024;
                assert description.getPartitionWriteBurstBytes() == 4 * 1024 * 1024;
                assert description.getConsumers().size() == 2;
                assert description.getSupportedCodecs().getCodecs().size() == 1;
                assert description.getSupportedCodecs().getCodecs().contains(Codec.LZOP);
            }

            {
                // Alter topic 2
                Map<String, String> attrs = new HashMap<>();
                attrs.put("attrName4", "attrValue4");
                attrs.put("attrName5", "attrValue5");

                topicClient.alterTopic(topicPath, AlterTopicSettings.newBuilder()
                                .setAlterPartitioningSettings(AlterPartitioningSettings.newBuilder()
                                        .setPartitionCountLimit(8)
                                        .build())
                                .setRetentionPeriod(Duration.ofHours(13))
                                .setRetentionStorageMb(512)
                                .setSupportedCodecs(SupportedCodecs.newBuilder()
                                        .addCodec(Codec.GZIP)
                                        .addCodec(Codec.ZSTD)
                                        .build())
                                .addAddConsumer(Consumer.newBuilder()
                                        .setName("testConsumer3")
                                        .setAttributes(attrs)
                                        .addAttribute("attrName6", "attrValue6")
                                        .setImportant(false)
                                        .setReadFrom(Instant.now().minus(Duration.ofHours(10)))
                                        .build())
                                .addDropConsumer("testConsumer1")
                                .addAlterConsumer(AlterConsumerSettings.newBuilder()
                                        .setName("testConsumer2")
                                        .setSupportedCodecs(SupportedCodecs.newBuilder()
                                                .addCodec(Codec.LZOP)
                                                .build())
                                        .addAlterAttribute("attrName7", "attrValue7")
                                        .addAlterAttribute("attrName3", null)
                                        .build())
                                .build())
                        .join()
                        .expectSuccess("cannot alter topic: " + topicPath);
            }

            {
                // Describe topic after second alter
                TopicDescription description = describeTopic(topicPath, topicClient);

                assert description.getPartitioningSettings().getMinActivePartitions() == 4;
                assert description.getPartitioningSettings().getPartitionCountLimit() == 8;
                assert Objects.equals(description.getRetentionPeriod(), Duration.ofHours(13));
                assert description.getRetentionStorageMb() == 512;
                assert description.getPartitionWriteSpeedBytesPerSecond() == 2 * 1024 * 1024;
                assert description.getPartitionWriteBurstBytes() == 4 * 1024 * 1024;
                assert description.getSupportedCodecs().getCodecs().size() == 2;
                assert description.getSupportedCodecs().getCodecs().contains(Codec.GZIP);
                assert description.getSupportedCodecs().getCodecs().contains(Codec.ZSTD);
                List<Consumer> consumers = description.getConsumers();
                assert consumers.size() == 2;
                for (Consumer consumer : consumers) {
                    String name = consumer.getName();
                    assert name.equals("testConsumer2") || name.equals("testConsumer3");
                    if (name.equals("testConsumer2")) {
                        Map<String, String> attrs = consumer.getAttributes();
                        assert attrs.size() == 3;
                        assert attrs.containsKey("attrName1");
                        assert attrs.containsKey("attrName2");
                        assert attrs.containsKey("attrName7");
                        assert attrs.get("attrName1").equals("attrValue1");
                        SupportedCodecs codecs = consumer.getSupportedCodecs();
                        assert codecs != null;
                        assert codecs.getCodecs().size() == 1;
                        assert codecs.getCodecs().contains(Codec.LZOP);
                    }
                }
            }

            {
                // Drop topic
                topicClient.dropTopic(topicPath, DropTopicSettings.newBuilder()
                                .withOperationTimeout(Duration.ofSeconds(3))
                                .withRequestTimeout(Duration.ofSeconds(4))
                                .build())
                        .join()
                        .expectSuccess("cannot drop topic: " + topicPath);
                transport.close();
            }
        }

        logger.info("ControlPlane finished");
    }

    public TopicDescription describeTopic(String topicPath, TopicClient topicClient) {
        // Describe topic after first alter
        Result<TopicDescription> topicDescriptionResult = topicClient.describeTopic(topicPath)
                .join();
        topicDescriptionResult.getStatus().expectSuccess("cannot describe topic: " + topicPath);
        TopicDescription description = topicDescriptionResult.getValue();

        StringBuilder message = new StringBuilder()

                .append("\n<========== ").append(topicPath).append(" ============>\n")
                .append("Partitioning settings:\n")
                .append("  ActivePartitions: ").append(description.getPartitioningSettings().getMinActivePartitions())
                    .append("\n")
                .append("  PartitionCountLimit: ").append(description.getPartitioningSettings().getMinActivePartitions())
                    .append("\n")
                .append("getPartitionWriteSpeedBytesPerSecond: ")
                    .append(description.getPartitionWriteSpeedBytesPerSecond()).append("\n")
                .append("getPartitionWriteBurstBytes: ").append(description.getPartitionWriteBurstBytes())
                    .append("\n")
                .append("Retention period: ").append(description.getRetentionPeriod()).append("\n")
                .append("Retention storage mb: ").append(description.getRetentionStorageMb()).append("\n");

        message.append("Supported codecs:\n");
        List<Codec> supportedCodecs = description.getSupportedCodecs().getCodecs();
        if (supportedCodecs.isEmpty()) {
            message.append("  none\n");
        }
        for (Codec codec : supportedCodecs) {
            message.append("  ").append(codec).append("\n");
        }

        message.append("Consumers:\n");
        for (Consumer consumer : description.getConsumers()) {
            message.append("  ").append(consumer.getName()).append(":\n")
                    .append("    Important: ").append(consumer.isImportant()).append("\n")
                    .append("    Read from: ").append(consumer.getReadFrom()).append("\n")
                    .append("    Supported codecs: \n");
            List<Codec> codecs = consumer.getSupportedCodecs().getCodecs();
            if (codecs.isEmpty()) {
                message.append("      none\n");
            }
            for (Codec codec : codecs) {
                message.append("      ").append(codec).append("\n");
            }

            message.append("    Attributes: \n");
            Map<String, String> attrs = consumer.getAttributes();
            if (attrs.isEmpty()) {
                message.append("      none\n");
            }
            for (Map.Entry<String, String> attr : attrs.entrySet()) {
                message.append("      ").append(attr.getKey()).append(": ").append(attr.getValue()).append("\n");
            }
        }
        message.append("<=========================================>");

        logger.info(message.toString());

        return description;
    }

    public static void main(String[] args) {
        new ControlPlane().doMain(args);
    }
}
