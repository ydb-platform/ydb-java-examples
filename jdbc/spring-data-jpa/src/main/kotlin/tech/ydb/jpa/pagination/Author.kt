package tech.ydb.jpa.pagination

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table

@Entity
@Table(name = "authors")
class Author {

    @Id
    lateinit var id: String

    lateinit var firstName: String

    lateinit var lastName: String
}
