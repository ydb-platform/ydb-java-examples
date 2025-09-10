package tech.ydb.testdb

import jakarta.persistence.EntityManager
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.concurrent.thread

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

    @Value("\${workers.count}")
    var workersCount: Int = 0

    override fun run(vararg args: String?) {
        val testString = String(ByteArray(1024) { 'a'.code.toByte() })

        repeat(workersCount) {
            thread {
                var sink = 0
                val t0 = System.nanoTime()
                val end = t0 + 10_000_000_000L
                var count = 0
                while (System.nanoTime() < end) {
                    sink += getFixedString(testString).length
                    count++
                }
                val elapsed = System.nanoTime() - t0
                val avgMs = elapsed.toDouble() / count / 1_000_000.0
                val opsPerSec = count * 1e9 / elapsed
                log.info("WorkerNum {}, avg={} ms/op, throughput={} ops/s", it, avgMs, opsPerSec)
            }
        }
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