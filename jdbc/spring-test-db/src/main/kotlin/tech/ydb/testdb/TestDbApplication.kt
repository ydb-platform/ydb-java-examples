package tech.ydb.testdb

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import javax.sql.DataSource
import java.util.Locale
import kotlin.math.floor
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
    lateinit var dataSource: DataSource

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
                dataSource.connection.use { connection ->
                    connection.isReadOnly = true
                    connection.prepareStatement("SELECT ?").use { statement ->
                        while (System.nanoTime() < end) {
                            statement.setString(1, testString)
                            statement.executeQuery().use { rs ->
                                if (rs.next()) {
                                    sink += rs.getString(1).length
                                }
                            }
                            count++
                        }
                    }
                }
                val elapsed = System.nanoTime() - t0
                val avgMs = elapsed.toDouble() / count / 1_000_000.0
                val opsPerSec = count * 1e9 / elapsed
                val avgMsStr = String.format(Locale.US, "%.3f", avgMs)
                val opsPerSecInt = floor(opsPerSec).toInt()
                log.info("WorkerNum {}, avg={} ms/op, throughput={} ops/s", it, avgMsStr, opsPerSecInt)
            }
        }
    }
}

fun main(args: Array<String>) {
    runApplication<TestDbApplication>(*args)
}