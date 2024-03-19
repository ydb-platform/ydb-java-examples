package tech.ydb.liquibase

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tech.ydb.liquibase.model.Employee
import tech.ydb.liquibase.repository.EmployeeRepository
import tech.ydb.liquibase.repository.findByIdOrNull
import tech.ydb.test.junit5.YdbHelperExtension
import java.math.BigDecimal
import java.time.LocalDate
import java.time.ZoneId
import java.util.*

/**
 * @author Kirill Kurdyukov
 */
@SpringBootTest
class YdbLiquibaseTest {

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

    @Autowired
    lateinit var employeeRepository: EmployeeRepository

    @Test
    fun `migration liquibase and CRUD actions`() {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("UTC")))

        val employee = Employee(
            1,
            "Kirill Kurdyukov",
            "kurdyukov-kir@ydb.tech",
            LocalDate.parse("2023-12-20"),
            BigDecimal("500000.000000000"),
            true,
            "YDB AppTeam",
            23
        )

        employeeRepository.save(employee)

        assertEquals(employee, employeeRepository.findByIdOrNull(employee.id))

        employeeRepository.delete(employee)

        assertNull(employeeRepository.findByIdOrNull(employee.id))
    }
}