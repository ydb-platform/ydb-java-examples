package tech.ydb.jpa.pagination

import java.util.*
import javax.persistence.*

@Entity
@Table(name = "books")
class Book {

    @Id
    lateinit var id: String

    lateinit var title: String
    lateinit var isbn10: String
    lateinit var publicationDate: Date

    @ManyToOne
    lateinit var author: Author
}
