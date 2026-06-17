package tech.ydb.slo.springjdbc;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import tech.ydb.slo.core.Launcher;

@Component
public class SloWorkloadRunner implements ApplicationRunner {
    private final SpringJdbcKvClient client;
    private final SloExitCodeHolder exitCodeHolder;

    public SloWorkloadRunner(
            SpringJdbcKvClient client,
            SloExitCodeHolder exitCodeHolder
    ) {
        this.client = client;
        this.exitCodeHolder = exitCodeHolder;
    }

    @Override
    public void run(ApplicationArguments args) {
        int exitCode = Launcher.run(
                "ydb-slo-spring-data-jdbc-workload",
                "java-spring-data-jdbc-kv",
                args.getSourceArgs(),
                (config, params, tablePath) -> client.forTable(tablePath)
        );
        exitCodeHolder.setExitCode(exitCode);
    }
}
