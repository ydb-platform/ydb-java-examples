package tech.ydb.testdb

import jakarta.persistence.EntityManager
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.time.Instant

/**
 * @author Kirill Kurdyukov
 */
@SpringBootApplication
class TestDbApplication : CommandLineRunner {
    companion object {
        private val log = LoggerFactory.getLogger(TestDbApplication::class.java)
    }

    @Autowired
    lateinit var entityManager: EntityManager

    override fun run(vararg args: String?) {
        val testString = String(ByteArray(1024) { 'a'.code.toByte() })

        val start = Instant.now()
        val end = start.plusSeconds(10)
        var step = start
        var count = 0
        while (step < end) {
            getFixedString(testString)
            count++
            step = Instant.now()
        }
        val duration = step.toEpochMilli() - start.toEpochMilli()

        log.info("Throughput = {}", duration.takeIf { it != 0L }?.let { count * 1000.0 / it } ?: 0.0)
    }

    fun getFixedString(s: String): String {
        return entityManager.createNativeQuery("SELECT ?1")
                .setParameter(1, s)
                .singleResult.toString()
    }
}

fun main(args: Array<String>) {
    runApplication<TestDbApplication>(*args)
}