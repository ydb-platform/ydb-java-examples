package kg.urmat.ydbtodolist

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@SpringBootApplication
@Import(AppConfig::class)
class YdbTodoListApplication


fun main(args: Array<String>) {
    runApplication<YdbTodoListApplication>(*args)
}
