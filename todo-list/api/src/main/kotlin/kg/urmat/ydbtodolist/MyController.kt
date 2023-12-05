package kg.urmat.ydbtodolist

import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam

@RequestMapping(path = ["/api"],
    produces = [MediaType.APPLICATION_JSON_VALUE])
interface MyController {
    @GetMapping("/getAll")
    @CrossOrigin(origins = ["http://localhost:3000"])
    fun getAll(): List<MyControllerImpl.Todo>

    @GetMapping("/create")
    @CrossOrigin(origins = ["http://localhost:3000"])
    fun createTodo(@RequestParam id: Long, @RequestParam title: String, @RequestParam text: String)

    @GetMapping("/createTables")
    @CrossOrigin(origins = ["http://localhost:3000"])
    fun createTables()
}
