package tech.ydb.jooq.configuration

import org.springframework.boot.autoconfigure.jooq.JooqAutoConfiguration
import org.springframework.context.annotation.Configuration
import tech.ydb.jooq.impl.YdbDSLContextImpl

/**
 * @author Kirill Kurdyukov
 */
@Configuration
class YdbJooqConfiguration : JooqAutoConfiguration.DslContextConfiguration() {

    override fun dslContext(configuration: org.jooq.Configuration): YdbDSLContextImpl {
        return YdbDSLContextImpl(configuration)
    }
}