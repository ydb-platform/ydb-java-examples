package tech.ydb.examples.topic;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.topic.write.QueueOverflowException;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.description.MetadataItem;
import tech.ydb.topic.description.TopicDescription;
import tech.ydb.topic.description.SupportedCodecs;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;
import tech.ydb.topic.settings.AlterTopicSettings;
import tech.ydb.topic.settings.CommitOffsetSettings;
import tech.ydb.topic.settings.CreateTopicSettings;
import tech.ydb.topic.settings.PartitioningSettings;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.ReceiveSettings;
import tech.ydb.topic.settings.SendSettings;
import tech.ydb.topic.settings.StartPartitionSessionSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.UpdateOffsetsInTransactionSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;
import tech.ydb.topic.write.WriteAck;

/** Executable source of the Java topic snippets on ydb.tech. */
public final class TopicExample {
    private static final List<String> EXPECTED = Arrays.asList(
            "11", "22", "33", "message", "message-data", "message-data");
    private static final String[] CONSUMERS = {
        "one", "commit_one", "batch", "commit_batch", "commit_each", "offset", "selectors", "outside"
    };
    private static final AtomicReference<Throwable> FAILURE = new AtomicReference<>();
    private static final ExampleLogger logger = new ExampleLogger();
    private static volatile Collector activeCollector;
    private static volatile java.util.function.Consumer<tech.ydb.topic.read.Message> processor;

    private TopicExample() { }

    public static void main(String[] args) throws Exception {
        String connString = System.getenv("YDB_CONNECTION_STRING");
        if (connString == null) connString = "grpc://localhost:2136/local";
        initializeTransport(connString);
        String topicPath = "ydb_tech_" + UUID.randomUUID().toString().replace("-", "");
        try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron()).build();
                TopicClient topicClient = TopicClient.newClient(transport).build();
                TableClient tableClient = TableClient.newClient(transport).build()) {
            initializeTopicClient(transport);
            createTopic(topicClient, topicPath);
            try {
                CompletableFuture<Status> alteration =
                // [BEGIN topic_alter]
                topicClient.alterTopic(topicPath, AlterTopicSettings.newBuilder()
                                .addAddConsumer(Consumer.newBuilder()
                                        .setName("new-consumer")
                                        .setSupportedCodecs(SupportedCodecs.newBuilder()
                                                .addCodec(Codec.RAW)
                                                .addCodec(Codec.GZIP)
                                                .build())
                                        .build())
                                .build());
                // [END topic_alter]
                alteration.join().expectSuccess();
                // [BEGIN topic_describe]
                Result<TopicDescription> topicDescriptionResult = topicClient.describeTopic(topicPath)
                        .join();
                TopicDescription description = topicDescriptionResult.getValue();
                // [END topic_describe]
                require(description.getConsumers().size() == CONSUMERS.length + 1, "Unexpected consumers");
                write(topicClient, topicPath);
                initializeSyncWriter(topicClient, topicPath);
                readSync(topicClient, topicPath, "one", false);
                readSync(topicClient, topicPath, "commit_one", true);
                readAsync(topicClient, topicPath, "batch", new Handler());
                readAsync(topicClient, topicPath, "commit_batch", new BatchCommitHandler());
                readAsync(topicClient, topicPath, "commit_each", new CommittingHandler());
                readAsync(topicClient, topicPath, "offset", new OffsetHandler());
                metadata(topicClient, topicPath + "_metadata");
                readWithoutConsumer(topicClient, topicPath);
                readSelectors(topicClient, topicPath);
                commitOutside(topicClient, topicPath);
                transactions(topicClient, tableClient, topicPath + "_tx");
            } finally {
                dropTopic(topicClient, topicPath);
            }
        }
        if (FAILURE.get() != null) throw new IllegalStateException("A callback failed", FAILURE.get());
        System.out.println("All topic scenarios completed");
    }

    private static void initializeTransport(String connString) {
        // [BEGIN topic_init]
        try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build()) {
            // Use YDB transport
        }
        // [END topic_init]
    }

    private static void initializeTopicClient(GrpcTransport transport) {
        ExecutorService compressionExecutor = Executors.newSingleThreadExecutor();
        // [BEGIN topic_client]
        try (TopicClient topicClient = TopicClient.newClient(transport)
                      .setCompressionExecutor(compressionExecutor)
                      .build()) {
          // Use topic client
        }
        // [END topic_client]
        compressionExecutor.shutdown();
    }

    private static void dropTopic(TopicClient topicClient, String topicPath) {
        CompletableFuture<Status> dropping =
        // [BEGIN topic_drop]
        topicClient.dropTopic(topicPath);
        // [END topic_drop]
        dropping.join().expectSuccess();
    }

    private static void createTopic(TopicClient topicClient, String topicPath) {
        CompletableFuture<Status> creation =
        // [BEGIN topic_create]
        topicClient.createTopic(topicPath, CreateTopicSettings.newBuilder()
                        // Optional
                        .setSupportedCodecs(SupportedCodecs.newBuilder()
                                .addCodec(Codec.RAW)
                                .addCodec(Codec.GZIP)
                                .build())
                        // Optional
                        .setPartitioningSettings(PartitioningSettings.newBuilder()
                                .setMinActivePartitions(3)
                                .build())
                        .build());
        // [END topic_create]
        creation.join().expectSuccess();
        AlterTopicSettings.Builder setup = AlterTopicSettings.newBuilder();
        for (String name : CONSUMERS) setup.addAddConsumer(Consumer.newBuilder().setName(name).build());
        topicClient.alterTopic(topicPath, setup.build()).join().expectSuccess();
    }

    private static WriterSettings writerSettings(String topicPath) {
        // [BEGIN topic_writer_settings]
        String producerAndGroupID = "group-id";
        WriterSettings settings = WriterSettings.newBuilder()
              .setTopicPath(topicPath)
              .setProducerId(producerAndGroupID)
              .setMessageGroupId(producerAndGroupID)
              .build();
        // [END topic_writer_settings]
        return settings;
    }

    private static void initializeSyncWriter(TopicClient topicClient, String topicPath) throws Exception {
        SyncWriter writer = topicClient.createSyncWriter(writerSettings(topicPath));
        try {
            // [BEGIN topic_sync_writer_init_wait]
            try {
                writer.initAndWait();
                logger.info("Init finished successfully");
            } catch (Exception exception) {
                logger.error("Exception while initializing writer: ", exception);
                return;
            }
            // [END topic_sync_writer_init_wait]
        } finally {
            writer.shutdown(30, TimeUnit.SECONDS);
        }
    }

    private static void write(TopicClient topicClient, String topicPath) throws Exception {
        WriterSettings settings = writerSettings(topicPath);
        // [BEGIN topic_sync_writer]
        SyncWriter writer = topicClient.createSyncWriter(settings);
        // [END topic_sync_writer]
        try {
            // [BEGIN topic_sync_writer_init]
            writer.init();
            // [END topic_sync_writer_init]
            // [BEGIN topic_write_sync]
            writer.send(Message.of("11".getBytes()));

            long timeoutSeconds = 5; // How long should we wait for a message to be put into sending buffer
            try {
              writer.send(
                      Message.newBuilder()
                              .setData("22".getBytes())
                              .setCreateTimestamp(Instant.now().minusSeconds(5))
                              .build(),
                      timeoutSeconds,
                      TimeUnit.SECONDS
              );
            } catch (TimeoutException exception) {
              logger.error("Send queue is full. Couldn't put message into sending queue within {} seconds", timeoutSeconds);
            } catch (InterruptedException | ExecutionException exception) {
              logger.error("Couldn't put the message into sending queue due to exception: ", exception);
            }
            // [END topic_write_sync]
            writer.flush();
        } finally {
            writer.shutdown(30, TimeUnit.SECONDS);
        }
        writeAsync(topicClient, topicPath);
        writeCompressed(topicClient, topicPath);
    }

    private static void writeAsync(TopicClient topicClient, String topicPath) throws Exception {
        WriterSettings settings = writerSettings(topicPath);
        // [BEGIN topic_async_writer]
        AsyncWriter writer = topicClient.createAsyncWriter(settings);

        // Init in background
        writer.init()
                .thenRun(() -> logger.info("Init finished successfully"))
                .exceptionally(ex -> {
                    logger.error("Init failed with ex: ", ex);
                    return null;
                });
        // [END topic_async_writer]
        try {
            // [BEGIN topic_write_async]
            try {
              // Non-blocking. Throws QueueOverflowException if send queue is full
              writer.send(Message.of("33".getBytes()));
            } catch (QueueOverflowException exception) {
              // Send queue is full. Need to retry with backoff or skip
            }
            // [END topic_write_async]
            byte[] message = bytes("message");
            CompletableFuture<WriteAck> acknowledgement =
            // [BEGIN topic_write_ack]
            writer.send(Message.of(message))
                  .whenComplete((result, ex) -> {
                      if (ex != null) {
                          logger.error("Exception on writing message message: ", ex);
                      } else {
                          switch (result.getState()) {
                              case WRITTEN:
                                  WriteAck.Details details = result.getDetails();
                                  StringBuilder str = new StringBuilder("Message was written successfully");
                                  if (details != null) {
                                      str.append(", offset: ").append(details.getOffset());
                                  }
                                  logger.debug(str.toString());
                                  break;
                              case ALREADY_WRITTEN:
                                  logger.warn("Message has already been written");
                                  break;
                              default:
                                  break;
                          }
                      }
                  });
            // [END topic_write_ack]
            acknowledgement.get(30, TimeUnit.SECONDS);
            // [BEGIN topic_write_metadata]
            List<MetadataItem> metadataItems = Arrays.asList(
                    new MetadataItem("meta-key", "meta-value".getBytes()),
                    new MetadataItem("another-key", "value".getBytes())
            );
            writer.send(
                    Message.newBuilder().setData(bytes("message-data"))
                            .setMetadataItems(metadataItems)
                            .build()
            );
            // [END topic_write_metadata]
            // [BEGIN topic_write_metadata_add]
            writer.send(
                    Message.newBuilder().setData(bytes("message-data"))
                            .addMetadataItem(new MetadataItem("meta-key", "meta-value".getBytes()))
                            .addMetadataItem(new MetadataItem("another-key", "value".getBytes()))
                            .build()
            );
            // [END topic_write_metadata_add]
        } finally {
            writer.shutdown().get(30, TimeUnit.SECONDS);
        }
    }

    private static void writeCompressed(TopicClient topicClient, String topicPath) throws Exception {
        topicPath += "_codec";
        topicClient.createTopic(topicPath, CreateTopicSettings.newBuilder()
                .setSupportedCodecs(SupportedCodecs.newBuilder().addCodec(Codec.ZSTD).build())
                .addConsumer(Consumer.newBuilder().setName("codec").build()).build()).join().expectSuccess();
        // [BEGIN topic_codec]
        String producerAndGroupID = "group-id";
        WriterSettings settings = WriterSettings.newBuilder()
                .setTopicPath(topicPath)
                .setProducerId(producerAndGroupID)
                .setMessageGroupId(producerAndGroupID)
                .setCodec(Codec.ZSTD)
                .build();
        // [END topic_codec]
        AsyncWriter writer = topicClient.createAsyncWriter(settings);
        try {
            writer.init().get(30, TimeUnit.SECONDS);
            writer.send(Message.of(bytes("compressed"))).get(30, TimeUnit.SECONDS);
        } finally {
            writer.shutdown().get(30, TimeUnit.SECONDS);
        }
        SyncReader reader = topicClient.createSyncReader(readerSettings(topicPath, "codec"));
        try {
            reader.initAndWait();
            require(Arrays.equals(reader.receive(30, TimeUnit.SECONDS).getData(), bytes("compressed")),
                    "Unexpected compressed payload");
        } finally {
            reader.shutdown();
            topicClient.dropTopic(topicPath).join().expectSuccess();
        }
    }

    private static ReaderSettings readerSettings(String topicPath, String consumerName) {
        // [BEGIN topic_reader_settings]
        ReaderSettings settings = ReaderSettings.newBuilder()
              .setConsumerName(consumerName)  // name of the consumer registered on the topic
              .addTopic(TopicReadSettings.newBuilder()
                      .setPath(topicPath)
                      .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // read from this timestamp (optional)
                      .setMaxLag(Duration.ofMinutes(30)) // maximum lag from the end of the queue (optional)
                      .build())
              .build();
        // [END topic_reader_settings]
        return settings;
    }

    private static void readSync(TopicClient topicClient, String topicPath, String consumer, boolean commit)
            throws Exception {
        ReaderSettings settings = readerSettings(topicPath, consumer);
        // [BEGIN topic_sync_reader]
        SyncReader reader = topicClient.createSyncReader(settings);
        // [END topic_sync_reader]
        List<String> received = new ArrayList<>();
        processor = message -> {
            received.add(checkMessage(message));
            if (commit) commitOne(message);
            if (received.size() == EXPECTED.size()) throw new ScenarioComplete();
        };
        try {
            // [BEGIN topic_sync_reader_init]
            try {
                reader.initAndWait();
                logger.info("Init finished successfully");
            } catch (Exception exception) {
                logger.error("Exception while initializing reader: ", exception);
                return;
            }
            // [END topic_sync_reader_init]
            try {
                // [BEGIN topic_read_one]
                while(true) {
                  tech.ydb.topic.read.Message message = reader.receive();
                  process(message);
                }
                // [END topic_read_one]
            } catch (ScenarioComplete complete) {
                require(counts(received).equals(counts(EXPECTED)), "Unexpected messages");
            }
        } finally {
            reader.shutdown();
            processor = null;
        }
    }

    private static void commitOne(tech.ydb.topic.read.Message message) {
        CompletableFuture<Void> completion =
        // [BEGIN topic_read_commit]
        message.commit()
               .whenComplete((result, ex) -> {
                   if (ex != null) {
                       // Read session was probably closed, there is nothing we can do here.
                       // Do not retry this commit on the same event.
                       logger.error("exception while committing message: ", ex);
                   } else {
                       logger.info("message committed successfully");
                   }
               });
        // [END topic_read_commit]
        completion.join();
    }

    private static void readAsync(TopicClient topicClient, String topicPath, String consumer, AbstractReadEventHandler delegate)
            throws Exception {
        ReaderSettings readerSettings = readerSettings(topicPath, consumer);
        // [BEGIN topic_handler_settings]
        ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
              .setEventHandler(new Handler())
              .build();
        // [END topic_handler_settings]
        Collector handler = new Collector(delegate, EXPECTED);
        processor = message -> checkMessage(message);
        activeCollector = handler;
        handlerSettings = ReadEventHandlersSettings.newBuilder().setEventHandler(handler).build();
        // [BEGIN topic_async_reader]
        AsyncReader reader = topicClient.createAsyncReader(readerSettings, handlerSettings);
        // Init in background
        reader.init()
              .thenRun(() -> logger.info("Init finished successfully"))
              .exceptionally(ex -> {
                  logger.error("Init failed with ex: ", ex);
                  return null;
              });
        // [END topic_async_reader]
        try {
            handler.awaitMessages();
            activeCollector = null;
        } finally {
            reader.shutdown().get(30, TimeUnit.SECONDS);
        }
    }

    private static void readSelectors(TopicClient topicClient, String topicPath) throws Exception {
        String anotherTopic = topicPath + "_another";
        String consumerName = "selectors";
        topicClient.createTopic(anotherTopic, CreateTopicSettings.newBuilder()
                .addConsumer(Consumer.newBuilder().setName(consumerName).build()).build()).join().expectSuccess();
        // [BEGIN topic_reader_selectors]
        ReaderSettings settings = ReaderSettings.newBuilder()
                .setConsumerName(consumerName)
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(topicPath)
                        .build())
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(anotherTopic)
                        .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // Optional
                        .setMaxLag(Duration.ofMinutes(30)) // Optional
                        .build())
                .build();
        // [END topic_reader_selectors]
        SyncReader reader = topicClient.createSyncReader(settings);
        try {
        // [BEGIN topic_sync_reader_background]
        reader.init();
        // [END topic_sync_reader_background]
            checkMessage(reader.receive(30, TimeUnit.SECONDS));
        } finally {
            reader.shutdown();
            topicClient.dropTopic(anotherTopic).join().expectSuccess();
        }
    }

    private static void readWithoutConsumer(TopicClient topicClient, String TOPIC_NAME) throws Exception {
        Collector handler = new Collector(new ConsumerlessHandler(), EXPECTED);
        processor = message -> checkMessage(message);
        activeCollector = handler;
        // [BEGIN topic_no_consumer]
        ReaderSettings settings = ReaderSettings.newBuilder()
                .withoutConsumer()
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(TOPIC_NAME)
                        .setPartitionIds(Arrays.asList(0L, 1L, 2L))
                        .build())
                .build();
        // [END topic_no_consumer]
        AsyncReader reader = topicClient.createAsyncReader(settings,
                ReadEventHandlersSettings.newBuilder().setEventHandler(handler).build());
        try {
            reader.init().get(30, TimeUnit.SECONDS);
            handler.awaitMessages();
            activeCollector = null;
        } finally {
            reader.shutdown().get(30, TimeUnit.SECONDS);
        }
    }

    private static void metadata(TopicClient topicClient, String topicPath) throws Exception {
        topicClient.createTopic(topicPath, CreateTopicSettings.newBuilder()
                .addConsumer(Consumer.newBuilder().setName("metadata").build()).build()).join().expectSuccess();
        AsyncWriter writer = topicClient.createAsyncWriter(writerSettings(topicPath));
        try {
            writer.init().get(30, TimeUnit.SECONDS);
            writer.send(Message.newBuilder().setData(bytes("message-data"))
                    .addMetadataItem(new MetadataItem("meta-key", bytes("meta-value"))).build()).join();
        } finally {
            writer.shutdown().get(30, TimeUnit.SECONDS);
        }
        SyncReader reader = topicClient.createSyncReader(readerSettings(topicPath, "metadata"));
        try {
            reader.initAndWait();
            // [BEGIN topic_read_metadata]
            tech.ydb.topic.read.Message message = reader.receive();
            List<MetadataItem> metadata = message.getMetadataItems();
            // [END topic_read_metadata]
            require(metadata.size() == 1, "Unexpected metadata");
            require(metadata.get(0).getKey().equals("meta-key"), "Unexpected metadata key");
        } finally {
            reader.shutdown();
            dropTopic(topicClient, topicPath);
        }
    }

    private static void commitOutside(TopicClient topicClient, String topicPath) throws Exception {
        SyncReader reader = topicClient.createSyncReader(readerSettings(topicPath, "outside"));
        try {
            reader.initAndWait();
            tech.ydb.topic.read.Message message = reader.receive(30, TimeUnit.SECONDS);
            checkMessage(message);
            long partitionID = message.getPartitionSession().getPartitionId();
            String consumer = "outside";
            long offset = message.getOffset() + 1;
            // [BEGIN topic_commit_outside]
            TopicClient client = topicClient;

            String sessionID = reader.getSessionId();
            // For AsyncReader, the session identifier can be obtained when processing the SessionStartedEvent

            client.commitOffset(
                topicPath,
                CommitOffsetSettings.newBuilder()
                    .setReadSessionId(sessionID)
                    .setPartitionId(partitionID)
                    .setConsumer(consumer)
                    .setOffset(offset)
                    .build()
            ).join().expectSuccess("Error commit!");
            // [END topic_commit_outside]
        } finally {
            reader.shutdown();
        }
    }

    private static void transactions(TopicClient topicClient, TableClient tableClient, String topicPath)
            throws Exception {
        topicClient.createTopic(topicPath, CreateTopicSettings.newBuilder()
                .addConsumer(Consumer.newBuilder().setName("sync").build())
                .addConsumer(Consumer.newBuilder().setName("async").build()).build()).join().expectSuccess();
        try {
            writeTxSync(topicClient, tableClient, topicPath);
            writeTxAsync(topicClient, tableClient, topicPath);
            SyncReader reader = topicClient.createSyncReader(readerSettings(topicPath, "sync"));
            try {
                reader.initAndWait();
                for (int index = 0; index < 2; index++) {
                    try (Session session = tableClient.createSession(Duration.ofSeconds(10)).join().getValue()) {
                        TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
                        transaction.executeDataQuery("SELECT 1").join().getValue();
                        // [BEGIN topic_read_tx_sync]
                        tech.ydb.topic.read.Message message = reader.receive(ReceiveSettings.newBuilder()
                              .setTransaction(transaction)
                              .build());
                        // [END topic_read_tx_sync]
                        require(Arrays.equals(message.getData(), bytes("Hello, world!")), "Unexpected transaction message");
                        transaction.commit().join().expectSuccess();
                    }
                }
            } finally {
                reader.shutdown();
            }
            TransactionHandler handler = new TransactionHandler(tableClient);
            Collector collector = new Collector(handler, Arrays.asList("Hello, world!", "Hello, world!"));
            AsyncReader asyncReader = topicClient.createAsyncReader(readerSettings(topicPath, "async"),
                    ReadEventHandlersSettings.newBuilder().setEventHandler(collector).build());
            handler.reader = asyncReader;
            try {
                asyncReader.init().get(30, TimeUnit.SECONDS);
                collector.awaitMessages();
            } finally {
                asyncReader.shutdown().get(30, TimeUnit.SECONDS);
            }
        } finally {
            dropTopic(topicClient, topicPath);
        }
    }

    private static void writeTxSync(TopicClient topicClient, TableClient tableClient, String topicPath) throws Exception {
        SyncWriter writer = topicClient.createSyncWriter(writerSettings(topicPath));
        try {
            writer.initAndWait();
            // [BEGIN topic_write_tx_sync]
            // creating a session in the table service
            Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
            if (!sessionResult.isSuccess()) {
              logger.error("Couldn't get a session from the pool: {}", sessionResult);
              return; // retry or shutdown
            }
            Session session = sessionResult.getValue();
            // creating a transaction in the table service
            // this transaction is not yet active and has no id
            TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

            // get message text within the transaction
            Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
                  .join();
            if (!dataQueryResult.isSuccess()) {
              logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
              return; // retry or shutdown
            }
            // now the transaction is active and has an id

            ResultSetReader rsReader = dataQueryResult.getValue().getResultSet(0);
            byte[] message;
            if (rsReader.next()) {
              message = rsReader.getColumn(0).getBytes();
            } else {
              return; // retry or shutdown
            }

            writer.send(
                  Message.of(message),
                  SendSettings.newBuilder()
                          .setTransaction(transaction)
                          .build()
            );

            // flush to wait until all messages reach server before commit
            writer.flush();

            Status commitStatus = transaction.commit().join();
            analyzeCommitStatus(commitStatus);
            // [END topic_write_tx_sync]
            session.close();
        } finally {
            writer.shutdown(30, TimeUnit.SECONDS);
        }
    }

    private static void writeTxAsync(TopicClient topicClient, TableClient tableClient, String topicPath) throws Exception {
        AsyncWriter writer = topicClient.createAsyncWriter(writerSettings(topicPath));
        int index = 0;
        try {
            writer.init().get(30, TimeUnit.SECONDS);
            // [BEGIN topic_write_tx_async]
            // creating a session in the table service
            Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
            if (!sessionResult.isSuccess()) {
              logger.error("Couldn't get a session from the pool: {}", sessionResult);
              return; // retry or shutdown
            }
            Session session = sessionResult.getValue();
            // creating a transaction in the table service
            // this transaction is not yet active and has no id
            TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

            // get message text within the transaction
            Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
                  .join();
            if (!dataQueryResult.isSuccess()) {
              logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
              return; // retry or shutdown
            }
            // now the transaction is active and has an id

            ResultSetReader rsReader = dataQueryResult.getValue().getResultSet(0);
            byte[] message;
            if (rsReader.next()) {
              message = rsReader.getColumn(0).getBytes();
            } else {
              return; // retry or shutdown
            }

            try {
              writer.send(Message.newBuilder()
                                      .setData(message)
                                      .build(),
                              SendSettings.newBuilder()
                                      .setTransaction(transaction)
                                      .build())
                      .whenComplete((result, ex) -> {
                          if (ex != null) {
                              logger.error("Exception while sending a message: ", ex);
                          } else {
                              switch (result.getState()) {
                                  case WRITTEN:
                                      WriteAck.Details details = result.getDetails();
                                      logger.info("Message was written successfully, offset: " + details.getOffset());
                                      break;
                                  case ALREADY_WRITTEN:
                                      logger.info("Message has already been written");
                                      break;
                                  default:
                                      break;
                              }
                          }
                      })
                      // Waiting for the message to reach the server before committing the transaction
                      .join();

              Status commitStatus = transaction.commit().join();
              analyzeCommitStatus(commitStatus);
            } catch (QueueOverflowException exception) {
              logger.error("Queue overflow exception while sending a message{}: ", index, exception);
              // Send queue is full. Need to retry with backoff or skip
            }
            // [END topic_write_tx_async]
            session.close();
        } finally {
            writer.shutdown().get(30, TimeUnit.SECONDS);
        }
    }

    private static void process(tech.ydb.topic.read.Message message) {
        processor.accept(message);
    }

    private static String checkMessage(tech.ydb.topic.read.Message message) {
        require(message != null, "Timed out waiting for a message");
        String payload = new String(message.getData(), StandardCharsets.UTF_8);
        require(EXPECTED.contains(payload), "Unexpected payload: " + payload);
        if (payload.equals("message-data")) {
            require(message.getMetadataItems().stream().anyMatch(item -> item.getKey().equals("meta-key")
                    && Arrays.equals(item.getValue(), bytes("meta-value"))), "Unexpected metadata");
        }
        return payload;
    }

    private static Map<String, Integer> counts(List<String> values) {
        Map<String, Integer> result = new HashMap<>();
        for (String value : values) result.merge(value, 1, Integer::sum);
        return result;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void analyzeCommitStatus(Status status) {
        status.expectSuccess();
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static final class ScenarioComplete extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    // [BEGIN topic_read_batch]
    private static class Handler extends AbstractReadEventHandler {
      @Override
      public void onMessages(DataReceivedEvent event) {
          for (tech.ydb.topic.read.Message message : event.getMessages()) {
              process(message);
          }
      }
    }
    // [END topic_read_batch]

    // [BEGIN topic_read_batch_individual_commit]
    private static class CommittingHandler extends AbstractReadEventHandler {
      @Override
      public void onMessages(DataReceivedEvent event) {
          for (tech.ydb.topic.read.Message message : event.getMessages()) {
              StringBuilder str = new StringBuilder();
              logger.info("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());

              process(message);

              message.commit().thenRun(() -> {
                  logger.info("Message committed");
              });
          }
      }
    }
    // [END topic_read_batch_individual_commit]

    private static class BatchCommitHandler extends AbstractReadEventHandler {
        // [BEGIN topic_read_batch_commit]
        @Override
        public void onMessages(DataReceivedEvent event) {
          for (tech.ydb.topic.read.Message message : event.getMessages()) {
              process(message);
          }
          event.commit()
                 .whenComplete((result, ex) -> {
                     if (ex != null) {
                         // Read session was probably closed, there is nothing we can do here.
                         // Do not retry this commit on the same message.
                         logger.error("exception while committing message batch: ", ex);
                     } else {
                         logger.info("message batch committed successfully");
                     }
                 });
        }
        // [END topic_read_batch_commit]
    }

    private static class OffsetHandler extends Handler {
        private final long lastReadOffset = 0L;
        private final long lastCommitOffset = 0L;
        // [BEGIN topic_client_offset]
        @Override
        public void onStartPartitionSession(StartPartitionSessionEvent event) {
            event.confirm(StartPartitionSessionSettings.newBuilder()
                    .setReadOffset(lastReadOffset) // Long
                    .setCommitOffset(lastCommitOffset) // Long
                    .build());
        }
        // [END topic_client_offset]
    }

    private static class ConsumerlessHandler extends Handler {
        private final long lastReadOffset = 0L;
        // [BEGIN topic_no_consumer_offset]
        @Override
        public void onStartPartitionSession(StartPartitionSessionEvent event) {
            event.confirm(StartPartitionSessionSettings.newBuilder()
                    .setReadOffset(lastReadOffset) // the last offset read by this client, Long
                    .build());
        }
        // [END topic_no_consumer_offset]
    }

    private static class SessionEvents extends AbstractReadEventHandler {
        @Override
        public void onMessages(DataReceivedEvent event) { }

        // [BEGIN topic_soft_stop]
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
        // [END topic_soft_stop]
        // [BEGIN topic_hard_stop]
        @Override
        public void onPartitionSessionClosed(PartitionSessionClosedEvent event) {
          logger.info("Partition session {} is closed.", event.getPartitionSession().getPartitionId());
        }
        // [END topic_hard_stop]
    }

    private static final class TransactionHandler extends AbstractReadEventHandler {
        private final TableClient tableClient;
        private AsyncReader reader;
        private TransactionHandler(TableClient tableClient) { this.tableClient = tableClient; }
        // [BEGIN topic_read_tx_async]
        @Override
        public void onMessages(DataReceivedEvent event) {
          for (tech.ydb.topic.read.Message message : event.getMessages()) {
              // creating a session in the table service
              Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
              if (!sessionResult.isSuccess()) {
                  logger.error("Couldn't get a session from the pool: {}", sessionResult);
                  return; // retry or shutdown
              }
              Session session = sessionResult.getValue();
              // creating a transaction in the table service
              // this transaction is not yet active and has no id
              TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

              // do something else in the transaction
              transaction.executeDataQuery("SELECT 1").join();
              // now the transaction is active and has an id
              // analyzeQueryResultIfNeeded();

              Status updateStatus = reader.updateOffsetsInTransaction(transaction,
                              message.getPartitionOffsets(), new UpdateOffsetsInTransactionSettings.Builder().build())
                      // Do not commit a transaction without waiting for updateOffsetsInTransaction result to avoid a race condition
                      .join();
              if (!updateStatus.isSuccess()) {
                  logger.error("Couldn't update offsets in a transaction: {}", updateStatus);
                  return; // retry or shutdown
              }

              Status commitStatus = transaction.commit().join();
              analyzeCommitStatus(commitStatus);
          }
        }
        // [END topic_read_tx_async]
    }

    private static final class Collector extends AbstractReadEventHandler {
        private final AbstractReadEventHandler delegate;
        private final List<String> expected;
        private final ConcurrentMap<String, Integer> received = new ConcurrentHashMap<>();
        private final CountDownLatch done;
        private final SessionEvents sessionEvents = new SessionEvents();
        private int pendingCommits;

        private Collector(AbstractReadEventHandler delegate, List<String> expected) {
            this.delegate = delegate;
            this.expected = expected;
            this.done = new CountDownLatch(expected.size());
        }

        @Override
        public void onMessages(DataReceivedEvent event) {
            try {
                synchronized (this) {
                    if (delegate instanceof CommittingHandler) pendingCommits += event.getMessages().size();
                    if (delegate instanceof BatchCommitHandler) pendingCommits++;
                }
                delegate.onMessages(event);
                for (tech.ydb.topic.read.Message message : event.getMessages()) {
                    String payload = new String(message.getData(), StandardCharsets.UTF_8);
                    require(expected.contains(payload), "Unexpected payload: " + payload);
                    received.merge(payload, 1, Integer::sum);
                    done.countDown();
                }
            } catch (Exception error) {
                FAILURE.compareAndSet(null, error);
                while (done.getCount() > 0) done.countDown();
            }
        }

        @Override
        public void onStartPartitionSession(StartPartitionSessionEvent event) {
            delegate.onStartPartitionSession(event);
        }

        @Override
        public void onStopPartitionSession(StopPartitionSessionEvent event) {
            sessionEvents.onStopPartitionSession(event);
        }

        @Override
        public void onPartitionSessionClosed(PartitionSessionClosedEvent event) {
            sessionEvents.onPartitionSessionClosed(event);
        }

        private void awaitMessages() throws Exception {
            require(done.await(30, TimeUnit.SECONDS), "Timed out waiting for asynchronous messages");
            require(received.equals(counts(expected)), "Unexpected asynchronous payloads");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            synchronized (this) {
                while (pendingCommits > 0 && FAILURE.get() == null) {
                    long remaining = deadline - System.nanoTime();
                    require(remaining > 0, "Timed out waiting for commit callbacks");
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                }
            }
            require(FAILURE.get() == null, "A reader callback failed");
        }

        private synchronized void commitCompleted() {
            pendingCommits--;
            notifyAll();
        }

        private synchronized void failed() { notifyAll(); }
    }

    private static final class ExampleLogger {
        private void print(String message, Object... args) {
            System.out.println(message + " " + Arrays.toString(args));
        }
        public void info(String message, Object... args) {
            print(message, args);
            Collector collector = activeCollector;
            if (collector != null && (message.equals("Message committed")
                    || message.equals("message batch committed successfully"))) collector.commitCompleted();
        }
        public void debug(String message, Object... args) { print(message, args); }
        public void warn(String message, Object... args) { print(message, args); }
        public void error(String message, Object... args) {
            print(message, args);
            Throwable error = args.length > 0 && args[args.length - 1] instanceof Throwable
                    ? (Throwable) args[args.length - 1] : new IllegalStateException(message);
            FAILURE.compareAndSet(null, error);
            Collector collector = activeCollector;
            if (collector != null) collector.failed();
        }
    }
}
