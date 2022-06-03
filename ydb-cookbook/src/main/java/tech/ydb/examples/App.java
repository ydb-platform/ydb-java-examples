package tech.ydb.examples;

import tech.ydb.core.grpc.GrpcTransport;



/**
 * @author Sergey Polovko
 */
public interface App extends AutoCloseable {

    void run();

    @Override
    void close();

    interface Factory {
        App newApp(GrpcTransport transport, String path);
    }
}
