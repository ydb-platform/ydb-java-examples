package tech.ydb.examples;

import java.time.Duration;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.DescribePathResult;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.description.SupportedCodecs;
import tech.ydb.topic.settings.CreateTopicSettings;


/**
 * @author Sergey Polovko
 * @author Nikolay Perfilov
 */
public abstract class SimpleExample {
    protected static final String TOPIC_NAME = System.getenv("YDB_TOPIC_NAME");
    protected static final String CONSUMER_NAME = System.getenv("YDB_CONSUMER_NAME");

    protected void doMain(String[] args) {
        if (args.length > 1) {
            System.err.println("Too many arguments");
            return;
        }
        String connString;
        if (args.length == 1) {
            connString = args[0];
        } else {
            connString = "some.host.name.com:2135?database=/Root";
            System.err.println("Pass <connection-string> as argument to override connection settings\n");
        }

        System.err.println("connection-string: " + connString + "\n");

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build()) {
            run(transport,
                    transport.getDatabase().endsWith("/")
                            ? transport.getDatabase()
                            : transport.getDatabase() + "/"
            );
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    protected abstract void run(GrpcTransport transport, String pathPrefix);
}
