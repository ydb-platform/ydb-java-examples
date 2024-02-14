package tech.ydb.jpa.simple

import jakarta.persistence.*
import org.hibernate.annotations.GenericGenerator
import org.hibernate.engine.spi.SharedSessionContractImplementor
import org.hibernate.id.IdentifierGenerator
import org.springframework.data.util.ProxyUtils
import java.util.concurrent.ThreadLocalRandom

@Entity
@Table(name = "Users", indexes = [Index(name = "username_index", columnList = "username")])
@NamedQuery(name = "User.findByTheUsersName", query = "from User u where u.username = ?1")
class User {

    @Id
    @GeneratedValue(generator = "random-int-id")
    @GenericGenerator(name = "random-int-id", type = RandomLongGenerator::class)
    var id: Long = 0

    lateinit var username: String

    lateinit var firstname: String

    lateinit var lastname: String

    class RandomLongGenerator : IdentifierGenerator {
        override fun generate(session: SharedSessionContractImplementor, `object`: Any): Any {
            return ThreadLocalRandom.current().nextLong()
        }
    }

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
