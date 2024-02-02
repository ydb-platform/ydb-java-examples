package tech.ydb.jpa

import org.junit.jupiter.api.extension.RegisterExtension
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tech.ydb.test.junit5.YdbHelperExtension

/**
 * @author Kirill Kurdyukov
 */
@ActiveProfiles("test", "ydb")
abstract class YdbDockerTest {

    companion object {
        @JvmField
        @RegisterExtension
        val ydb = YdbHelperExtension()

        @JvmStatic
        @DynamicPropertySource
        fun propertySource(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") {
                "jdbc:ydb:${if (ydb.useTls()) "grpcs://" else "grpc://"}" +
                        "${ydb.endpoint()}${ydb.database()}${ydb.authToken()?.let { "?token=$it" } ?: ""}"
            }
        }
    }
}