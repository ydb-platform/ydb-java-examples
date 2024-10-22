package tech.ydb.examples.topic.transactions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.topic.SimpleTopicExample;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.ReceiveSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 * @author Nikolay Perfilov
 */
public class TransactionReadSync extends SimpleTopicExample {

    @Override
    protected void run(GrpcTransport transport) {

        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {
            try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
                ReaderSettings settings = ReaderSettings.newBuilder()
                        .setConsumerName(CONSUMER_NAME)
                        .addTopic(TopicReadSettings.newBuilder()
                                .setPath(TOPIC_NAME)
                                .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                                .setMaxLag(Duration.ofMinutes(30))
                                .build())
                        .build();

                SyncReader reader = topicClient.createSyncReader(settings);

                // Init in background
                reader.init();

                tableAndTopicWithinTransaction(topicClient, queryClient, reader);
                tableAndTopicWithinTransaction(topicClient, queryClient, reader);

                reader.shutdown();
            }
        }
    }

    private void tableAndTopicWithinTransaction(TopicClient topicClient, QueryClient queryClient, SyncReader reader) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

        retryCtx.supplyStatus(querySession -> {
            // Begin new transaction on server
            QueryTransaction transaction = querySession.beginTransaction(TxMode.SERIALIZABLE_RW).join().getValue();

            // Read message in transaction
            Message message;
            try {
                message = reader.receive(ReceiveSettings.newBuilder()
                        .setTransaction(transaction)
                        .build());
            } catch (InterruptedException exception) {
                throw new RuntimeException("Interrupted exception while waiting for message");
            }

            // Execute a query in transaction
            Status queryStatus = transaction.createQuery(
                    "$last = SELECT MAX(val) FROM table WHERE id=$id;\n" +
                    "UPSERT INTO t (id, val) VALUES($id, COALESCE($last, 0) + $value)",
                    Params.of("$id", PrimitiveValue.newText(message.getMessageGroupId()),
                            "$value", PrimitiveValue.newInt64(Long.parseLong(
                                    new String(message.getData(), StandardCharsets.UTF_8)))))
                    .execute().join().getStatus();

            if (!queryStatus.isSuccess()) {
                return CompletableFuture.completedFuture(queryStatus);
            }

            // Return commit status to SessionRetryContext function
            return transaction.commit().thenApply(Result::getStatus);
        }).join().expectSuccess("Couldn't read from topic and write to table in transaction");
    }

    public static void main(String[] args) {
        new TransactionReadSync().doMain(args);
    }
}
