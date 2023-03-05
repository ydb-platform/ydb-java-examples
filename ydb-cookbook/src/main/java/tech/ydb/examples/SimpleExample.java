package tech.ydb.examples;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;


/**
 * @author Sergey Polovko
 * @author Nikolay Perfilov
 */
public abstract class SimpleExample {

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

        ExecutorService executor = Executors.newFixedThreadPool(3);
        try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
                .withCallExecutor(executor)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build()) {
            run(transport,
                    transport.getDatabase().endsWith("/")
                            ? transport.getDatabase()
                            : transport.getDatabase() + "/"
            );
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    protected abstract void run(GrpcTransport transport, String pathPrefix);
}
