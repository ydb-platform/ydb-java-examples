package tech.ydb.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

import tech.ydb.auth.iam.CloudAuthProvider;
import tech.ydb.core.auth.AuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.TableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;

import yandex.cloud.sdk.auth.provider.ApiKeyCredentialProvider;

public class CloudConnect {
    private CloudConnect() { }

    public static void main(String[] args) throws IOException {
        AuthProvider authProvider = CloudAuthProvider.newAuthProvider(ApiKeyCredentialProvider.builder()
                .fromFile(Paths.get(System.getProperty("user.home") + "/.ydb/sa_name.json"))
                .build());

        byte[] cert = Files.readAllBytes(Paths.get(System.getProperty("user.home") + "/.crt/root.crt"));

        GrpcTransport transport = GrpcTransport.forEndpoint(
                "lb.etn00ldc52ve3j9brsc4.ydb.mdb.yandexcloud.net:2135",
                "/ru-central1/b1grr8be1uqu58ilivmv/etn00ldc52ve3j9brsc4")
                .withAuthProvider(authProvider)
                .withSecureConnection(cert)
                .build();

        TableClient tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport))
                .build();

        tableClient.getOrCreateSession(Duration.ofSeconds(10))
                .join().expect("ok");
    }
}
