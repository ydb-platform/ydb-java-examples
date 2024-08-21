package tech.ydb.shaded.lock

import jakarta.annotation.PostConstruct
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tech.ydb.lock.provider.YdbCoordinationServiceLockProvider


/**
 * @author Kirill Kurdyukov
 */
@Component
class JobHandler {

    @Scheduled(cron = "* * * * * *")
    @SchedulerLock(name = "YDB Some Job", lockAtLeastFor = "15S", lockAtMostFor = "20S")
    fun awesomeJob() {
        for (i in 0..4) {
            println("Processing {$i}")

            Thread.sleep(1_000)
        }
    }
}