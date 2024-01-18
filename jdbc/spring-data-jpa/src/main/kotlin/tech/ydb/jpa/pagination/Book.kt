package tech.ydb.jpa.pagination

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.ManyToOne
import jakarta.persistence.Table
import java.util.*

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
