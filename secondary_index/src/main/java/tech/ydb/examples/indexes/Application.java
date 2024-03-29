package tech.ydb.examples.indexes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.examples.indexes.configuration.IndexesConfigurationProperties;
import tech.ydb.examples.indexes.repositories.SeriesRepository;
import tech.ydb.table.TableClient;

@SpringBootApplication
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Bean
    ExecutorService grpcExecutor() {
        return Executors.newFixedThreadPool(3);
    }

    @Bean
    ScheduledExecutorService timerScheduler() {
        return Executors.newScheduledThreadPool(1);
    }

    @Bean(destroyMethod = "close")
    GrpcTransport grpcTransport(IndexesConfigurationProperties properties, ExecutorService grpcExecutor) {
        String endpoint = properties.getEndpoint();
        String database = properties.getDatabase();
        String token = properties.getToken();
        if (token == null || token.isEmpty()) {
            token = System.getenv("YDB_TOKEN");
        }
        logger.info("Creating rpc transport for endpoint={} database={}", endpoint, database);
        GrpcTransportBuilder builder = GrpcTransport.forEndpoint(endpoint, database)
                .withCallExecutor(grpcExecutor);
        if (token != null && !token.isEmpty()) {
            builder.withAuthProvider(new TokenAuthProvider(token));
        }
        return builder.build();
    }

    @Bean
    TableClient tableClient(GrpcTransport transport) {
        return TableClient.newClient(transport).build();
    }

    @Bean
    SeriesRepository seriesRepository(TableClient tableClient, IndexesConfigurationProperties properties) {
        return new SeriesRepository(tableClient, properties.getPrefix());
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
