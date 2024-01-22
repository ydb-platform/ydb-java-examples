package tech.ydb.jpa

import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait

/**
 * Debug mode
 *
 * @author Kirill Kurdyukov
 */
@ActiveProfiles("test", "postgres")
abstract class PostgresDockerTest {

    companion object {
        private val postgresContainer: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:latest")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test")
                .waitingFor(Wait.forListeningPort())

        @JvmStatic
        @DynamicPropertySource
        fun prepareProperties(registry: DynamicPropertyRegistry) {
            postgresContainer.start()
            registry.add("spring.datasource.url", postgresContainer::getJdbcUrl)
            registry.add("spring.datasource.password", postgresContainer::getPassword)
            registry.add("spring.datasource.username", postgresContainer::getUsername)
        }
    }
}