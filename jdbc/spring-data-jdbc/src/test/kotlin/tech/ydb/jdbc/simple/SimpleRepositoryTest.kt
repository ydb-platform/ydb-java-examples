package tech.ydb.jdbc.simple

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.domain.Limit
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.transaction.annotation.Transactional
import tech.ydb.jdbc.YdbDockerTest

/**
 * @author Kirill Kurdyukov
 */
@SpringBootTest
@Transactional
class SimpleRepositoryTest : YdbDockerTest() {

    @Autowired
    lateinit var repository: SimpleUserRepository

    lateinit var user: User

    @BeforeEach
    fun setUp() {
        user = User().apply {
            username = "foobar"
            firstname = "firstname"
            lastname = "lastname"
        }
    }

    @Test
    fun findSavedUserById() {
        user = repository.save(user)

        assertThat(repository.findById(user.id)).hasValue(user)
    }

    @Test
    fun findSavedUserByLastname() {
        user = repository.save(user)

        assertThat(repository.findByLastname("lastname")).contains(user)
    }

    @Test
    fun findLimitedNumberOfUsersViaDerivedQuery() {
        (0..10).forEach { _ -> repository.save(User().apply { lastname = "lastname" }) }

        assertThat(repository.findByLastname("lastname", Limit.of(5))).hasSize(5)
    }

    @Test
    fun findLimitedNumberOfUsersViaAnnotatedQuery() {
        (0..10).forEach { _ -> repository.save(User().apply { firstname = "firstname" }) }

        assertThat(repository.findByFirstname("firstname", Limit.of(5))).hasSize(5)
    }

    @Test
    fun findByFirstnameOrLastname() {
        user = repository.save(user)

        assertThat(repository.findByFirstnameOrLastname("lastname")).contains(user)
    }

    @Test
    fun useOptionalAsReturnAndParameterType() {
        assertNull(repository.findByUsername("foobar"))

        repository.save(user)

        assertNotNull(repository.findByUsername("foobar"))
    }

    @Test
    fun useSliceToLoadContent() {
        val totalNumberUsers = 11
        val source: MutableList<User> = ArrayList(totalNumberUsers)

        for (i in 1..totalNumberUsers) {
            val user = User().apply {
                lastname = user.lastname
                username = "${user.lastname}-${String.format("%03d", i)}"
            }

            source.add(user)
        }

        repository.saveAll(source)

        val users = repository.findByLastnameOrderByUsernameAsc(user.lastname, PageRequest.of(1, 5))

        assertThat(users).containsAll(source.subList(5, 10))
    }

    @Test
    fun findFirst2ByOrderByLastnameAsc() {
        val user0 = User().apply { lastname = "lastname-0" }

        val user1 = User().apply { lastname = "lastname-1" }

        val user2 = User().apply { lastname = "lastname-2" }

        // we deliberately save the items in reverse
        repository.saveAll(listOf(user2, user1, user0))

        val result = repository.findFirst2ByOrderByLastnameAsc()

        assertThat(result).containsExactly(user0, user1)
    }

    @Test
    fun findTop2ByWithSort() {
        val user0 = User().apply { lastname = "lastname-0" }

        val user1 = User().apply { lastname = "lastname-1" }

        val user2 = User().apply { lastname = "lastname-2" }

        // we deliberately save the items in reverse
        repository.saveAll(listOf(user2, user1, user0))

        val resultAsc = repository.findTop2By(Sort.by(Sort.Direction.ASC, "lastname"))

        assertThat(resultAsc).containsExactly(user0, user1)

        val resultDesc = repository.findTop2By(Sort.by(Sort.Direction.DESC, "lastname"))

        assertThat(resultDesc).containsExactly(user2, user1)
    }

    @Test
    fun findByFirstnameOrLastnameUsingSpEL() {
        val first = User().apply { lastname = "lastname" }

        val second = User().apply { firstname = "firstname" }

        val third = User()

        repository.saveAll(listOf(first, second, third))

        val reference = User().apply { firstname = "firstname"; lastname = "lastname" }

        val users = repository.findByFirstnameOrLastname(reference)

        assertThat(users).contains(first)
        assertThat(users).contains(second)
        assertThat(users).hasSize(2)
    }
}