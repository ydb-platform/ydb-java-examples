package tech.ydb.slo.springjdbc;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import tech.ydb.slo.core.Config;

@Configuration
@EnableTransactionManagement
public class SloInfrastructureConfiguration {

    @Bean
    Config sloConfig() {
        return Config.fromEnv("java-spring-data-jdbc-kv");
    }

    @Bean
    DataSource sloDataSource(Config sloConfig) {
        DataSourceProperties properties = new DataSourceProperties();
        properties.setDriverClassName("tech.ydb.jdbc.YdbDriver");
        properties.setUrl(sloConfig.jdbcUrl());
        HikariDataSource dataSource = properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
        if (sloConfig.token() != null && !sloConfig.token().isEmpty()) {
            dataSource.addDataSourceProperty("token", sloConfig.token());
        }
        return dataSource;
    }

    @Bean
    JdbcTemplate sloJdbcTemplate(DataSource sloDataSource) {
        return new JdbcTemplate(sloDataSource);
    }
}
