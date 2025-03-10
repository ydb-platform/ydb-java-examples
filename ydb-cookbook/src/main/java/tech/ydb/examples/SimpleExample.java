package tech.ydb.examples;

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
            System.err.println("Pass <connection-string> as an argument. " +
                    "Example: some.host.name.com:2135?database=/Root\n");
            return;
        }

        System.out.println("connection-string: " + connString + "\n");

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
