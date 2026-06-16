package tech.ydb.slo.springjdbc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Spring Data JDBC SLO workload entry point.
 */
@SpringBootApplication
public class SloSpringDataJdbcApplication {

    public static void main(String[] args) {
        int exitCode = SpringApplication.exit(new SpringApplicationBuilder(SloSpringDataJdbcApplication.class)
                .web(WebApplicationType.NONE)
                .run(args));
        System.exit(exitCode);
    }
}
