package tech.ydb.examples.simple;

import java.util.concurrent.Executors;

import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.core.rpc.RpcTransport;


/**
 * @author Sergey Polovko
 */
public abstract class SimpleExample {

    private static final String HOST;
    private static final int PORT;
    private static final String PATH;

    static {
        HOST = System.getProperty("HOST", "solomon-dev-myt-00.search.yandex.net");
        PORT = Integer.parseInt(System.getProperty("PORT", "2135"));

        String path = System.getProperty("PATH", "/Solomon/");
        PATH = path.endsWith("/") ? path : path + "/";
    }

    protected void doMain() {
        System.err.println("Run with -DHOST=<hostname> and -DPORT=<port> to override connection settings");
        System.err.println();
        System.err.println("HOST=" + HOST);
        System.err.println("PORT=" + PORT);
        System.err.println("PATH=" + PATH);
        System.err.println();

        try (RpcTransport transport = GrpcTransportBuilder.singleHost(HOST, PORT)
            .withExecutorService(Executors.newFixedThreadPool(3))
            .build())
        {
            run(transport, PATH);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    abstract void run(RpcTransport transport, String pathPrefix);
}
