package tech.ydb.examples.simple;

import java.util.UUID;
import tech.ydb.core.grpc.GrpcTransport;

import tech.ydb.table.SchemeClient;
import tech.ydb.table.description.DescribePathResult;
import tech.ydb.table.description.ListDirectoryResult;
import tech.ydb.table.rpc.grpc.GrpcSchemeRpc;


/**
 * @author Sergey Polovko
 */
public class Scheme extends SimpleExample {

    @Override
    void run(GrpcTransport transport, String pathPrefix) {
        String directoryPath = pathPrefix + UUID.randomUUID().toString();
        SchemeClient schemeClient = SchemeClient.newClient(GrpcSchemeRpc.useTransport(transport)).build();

        schemeClient.makeDirectory(directoryPath)
            .join()
            .expectSuccess("cannot make directory: " + directoryPath);

        for (int i = 0; i < 3; i++) {
            String subDirectory = directoryPath + '/' + i;
            schemeClient.makeDirectory(subDirectory)
                .join()
                .expectSuccess("cannot make directory: " + subDirectory);
        }

        DescribePathResult result1 = schemeClient.describePath(directoryPath)
            .join()
            .getValue();

        System.out.println("--[self]---------------------------");
        System.out.println(result1.getSelf());
        System.out.println("-----------------------------------");

        ListDirectoryResult result2 = schemeClient.listDirectory(directoryPath)
            .join()
            .getValue();

        System.out.println("--[self]---------------------------");
        System.out.println(result2.getSelf());
        System.out.println("--[children]-----------------------");
        System.out.println(result2.getChildren());
        System.out.println("-----------------------------------");

        for (int i = 0; i < 3; i++) {
            String subDirectory = directoryPath + '/' + i;
            schemeClient.removeDirectory(subDirectory)
                .join()
                .expectSuccess("cannot remove directory: " + subDirectory);
        }

        schemeClient.removeDirectory(directoryPath)
            .join()
            .expectSuccess("cannot remove directory: " + directoryPath);

    }

    public static void main(String[] args) {
        new Scheme().doMain();
    }
}
