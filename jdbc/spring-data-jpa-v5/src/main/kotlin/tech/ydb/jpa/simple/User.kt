package tech.ydb.jpa.simple

import javax.persistence.*
import org.springframework.data.util.ProxyUtils

@Entity
@Table(name = "Users", indexes = [Index(name = "username_index", columnList = "username")])
@NamedQuery(name = "User.findByTheUsersName", query = "from User u where u.username = ?1")
class User {

    @Id
    var id: Long = 0

    lateinit var username: String

    lateinit var firstname: String

    lateinit var lastname: String

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
}
