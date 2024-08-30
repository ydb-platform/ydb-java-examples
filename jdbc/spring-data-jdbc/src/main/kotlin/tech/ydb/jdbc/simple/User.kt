package tech.ydb.jdbc.simple

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.util.ProxyUtils
import java.util.concurrent.ThreadLocalRandom

@Table(name = "Users")
class User : Persistable<Long> {

    @Id
    var id: Long = ThreadLocalRandom.current().nextLong()

    lateinit var username: String

    lateinit var firstname: String

    lateinit var lastname: String

    @Transient
    var new = true

    override fun equals(other: Any?): Boolean {
        if (null == other) {
            return false
        }

        if (this === other) {
            return true
        }

        if (javaClass != ProxyUtils.getUserClass(other)) {
            return false
        }

        val that: User = other as User

        return this.id == that.id
    }

    override fun hashCode(): Int {
        var hashCode = 17

        hashCode += id.hashCode() * 31

        return hashCode
    }

    override fun getId() = id
    override fun isNew() = new
}
