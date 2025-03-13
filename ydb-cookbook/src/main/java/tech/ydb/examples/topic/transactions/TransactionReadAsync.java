package tech.ydb.examples.topic.transactions;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import tech.ydb.topic.read.AsyncReader;
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
import tech.ydb.topic.settings.UpdateOffsetsInTransactionSettings;

/**
 * @author Nikolay Perfilov
 */
public class TransactionReadAsync extends SimpleTopicExample {
    private static final Logger logger = LoggerFactory.getLogger(TransactionReadAsync.class);
    private static final int MESSAGES_COUNT = 1;

    private final CompletableFuture<Void> messageReceivedFuture = new CompletableFuture<>();
    private QueryClient queryClient;
    private AsyncReader reader;

    @Override
    protected void run(GrpcTransport transport) {
        // WARNING: Working with transactions in Java Topic SDK is currently experimental. Interfaces may change
        try (TopicClient topicClient = TopicClient.newClient(transport).build()) {
            try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
                this.queryClient = queryClient;
                ReaderSettings readerSettings = ReaderSettings.newBuilder()
                        .setConsumerName(CONSUMER_NAME)
                        .addTopic(TopicReadSettings.newBuilder()
                                .setPath(TOPIC_NAME)
                                .build())
                        .build();

                ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
                        .setEventHandler(new Handler())
                        .build();

                reader = topicClient.createAsyncReader(readerSettings, handlerSettings);
                reader.init();
                messageReceivedFuture.join();
                reader.shutdown().join();
            }
        }
    }

    private class Handler extends AbstractReadEventHandler {
        private final AtomicInteger messageCounter = new AtomicInteger(0);

        @Override
        public void onMessages(DataReceivedEvent event) {
            for (Message message : event.getMessages()) {
                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                retryCtx.supplyStatus(querySession -> {
                    QueryTransaction transaction = querySession.beginTransaction(TxMode.SERIALIZABLE_RW)
                            .join().getValue();

                    // Update offsets in transaction
                    Status updateStatus =  reader.updateOffsetsInTransaction(transaction, message.getPartitionOffsets(),
                                    new UpdateOffsetsInTransactionSettings.Builder().build())
                            // Do not commit transaction without waiting for updateOffsetsInTransaction result
                            .join();
                    if (!updateStatus.isSuccess()) {
                        // Return update status to SessionRetryContext function
                        return CompletableFuture.completedFuture(updateStatus);
                    }

                    // Execute a query in transaction
                    Status queryStatus = transaction.createQuery(
                                    "DECLARE $id AS Uint64; \n" +
                                            "DECLARE $value AS Text;\n" +
                                            "UPSERT INTO table (id, value) VALUES ($id, $value)",
                                    Params.of("$id", PrimitiveValue.newUint64(message.getOffset()),
                                            "$value", PrimitiveValue.newText(new String(message.getData(),
                                                    StandardCharsets.UTF_8))))
                            .execute().join().getStatus();

                    if (!queryStatus.isSuccess()) {
                        // Return query status to SessionRetryContext function
                        return CompletableFuture.completedFuture(queryStatus);
                    }

                    // Return commit status to SessionRetryContext function
                    return transaction.commit().thenApply(Result::getStatus);
                }).join().expectSuccess("Couldn't read from topic and write to table in transaction");

                if (messageCounter.incrementAndGet() >= MESSAGES_COUNT) {
                    logger.info("{} messages committed in transaction. Finishing reading.", MESSAGES_COUNT);
                    messageReceivedFuture.complete(null);
                }
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
            messageReceivedFuture.complete(null);
        }
    }

    public static void main(String[] args) {
        new TransactionReadAsync().doMain(args);
    }
}
