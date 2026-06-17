package tech.ydb.slo.springjpa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class SloSpringDataJpaApplication {

    public static void main(String[] args) {
        int exitCode = SpringApplication.exit(new SpringApplicationBuilder(SloSpringDataJpaApplication.class)
                .web(WebApplicationType.NONE)
                .run(args));
        System.exit(exitCode);
    }
}
