package tech.ydb.jpa.pagination

import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

@Entity
@Table(name = "authors")
class Author {

    @Id
    lateinit var id: String

    lateinit var firstName: String

    lateinit var lastName: String
}
