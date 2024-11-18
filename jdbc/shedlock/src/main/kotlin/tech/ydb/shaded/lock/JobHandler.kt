package tech.ydb.shaded.lock

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * @author Kirill Kurdyukov
 */
@Component
class JobHandler {

    @Scheduled(cron = "* * * * * *")
    @SchedulerLock(name = "YDB Some Job", lockAtMostFor = "10S")
    fun awesomeJob() {
        println("PID LEADER: " + ProcessHandle.current().pid())

        for (i in 0..4) {
            println("Processing {$i}")

            Thread.sleep(1_000)
        }

        println("STOP LEADER")
    }
}