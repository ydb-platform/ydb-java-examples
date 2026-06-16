package tech.ydb.slo.springjpa;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import tech.ydb.slo.core.Launcher;

@Component
public class SloWorkloadRunner implements ApplicationRunner {
    private final SpringJpaKvClient client;
    private final SloExitCodeHolder exitCodeHolder;

    public SloWorkloadRunner(SpringJpaKvClient client, SloExitCodeHolder exitCodeHolder) {
        this.client = client;
        this.exitCodeHolder = exitCodeHolder;
    }

    @Override
    public void run(ApplicationArguments args) {
        int exitCode = Launcher.run(
                "ydb-slo-spring-data-jpa-workload",
                "java-spring-data-jpa-kv",
                args.getSourceArgs(),
                (config, params, tablePath) -> client.forTable(tablePath)
        );
        exitCodeHolder.setExitCode(exitCode);
    }
}
