package tech.ydb.slo;

import tech.ydb.slo.core.Launcher;
import tech.ydb.slo.query.QueryKvClient;

public final class Main {
    private Main() {

    }

    public static void main(String[] args) {
        Launcher.launch(
                "ydb-slo-query-workload",
                "java-query-kv",
                args,
                (config, params, tablePath) -> new QueryKvClient(config, params, tablePath)
        );
    }
}
