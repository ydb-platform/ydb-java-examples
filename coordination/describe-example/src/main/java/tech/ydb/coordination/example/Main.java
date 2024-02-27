package tech.ydb.coordination.example;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-coordination-api-example.jar <connection_url>");
            return;
        }

        try (GrpcTransport transport = GrpcTransport.forConnectionString(args[0])
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build()) {

            logger.info("run app example");
            App app = new App(transport, transport.getScheduler());
            app.run();
        }
    }
}
