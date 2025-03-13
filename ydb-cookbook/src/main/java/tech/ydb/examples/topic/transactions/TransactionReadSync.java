package tech.ydb.examples.topic.transactions;

import java.nio.charset.StandardCharsets;
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
        // WARNING: Working with transactions in Java Topic SDK is currently experimental. Interfaces may change
        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {
            try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
                ReaderSettings settings = ReaderSettings.newBuilder()
                        .setConsumerName(CONSUMER_NAME)
                        .addTopic(TopicReadSettings.newBuilder()
                                .setPath(TOPIC_NAME)
                                .build())
                        .build();

                SyncReader reader = topicClient.createSyncReader(settings);
                reader.init();

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                retryCtx.supplyStatus(querySession -> {
                    QueryTransaction transaction = querySession.beginTransaction(TxMode.SERIALIZABLE_RW).join().getValue();
                    Message message;
                    try {
                        message = reader.receive(ReceiveSettings.newBuilder()
                                .setTransaction(transaction)
                                .build());
                    } catch (InterruptedException exception) {
                        throw new RuntimeException("Interrupted exception while waiting for message");
                    }

                    Status queryStatus = transaction.createQuery(
                                    "DECLARE $id AS Uint64; \n" +
                                            "DECLARE $value AS Text;\n" +
                                            "UPSERT INTO table (id, value) VALUES ($id, $value)",
                                    Params.of("$id", PrimitiveValue.newUint64(message.getOffset()),
                                            "$value", PrimitiveValue.newText(new String(message.getData(),
                                                            StandardCharsets.UTF_8))))
                            .execute().join().getStatus();
                    if (!queryStatus.isSuccess()) {
                        return CompletableFuture.completedFuture(queryStatus);
                    }

                    return transaction.commit().thenApply(Result::getStatus);
                }).join().expectSuccess("Couldn't read from topic and write to table in transaction");
                reader.shutdown();
            }
        }
    }

    public static void main(String[] args) {
        new TransactionReadSync().doMain(args);
    }
}
