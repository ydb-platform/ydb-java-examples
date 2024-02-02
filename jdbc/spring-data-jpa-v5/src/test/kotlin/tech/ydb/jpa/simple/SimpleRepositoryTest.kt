package tech.ydb.jpa.simple

import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.InvalidDataAccessApiUsageException
import org.springframework.data.domain.PageRequest
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import tech.ydb.jpa.YdbDockerTest
import java.util.stream.Collectors

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
    fun removeByLastname() {
        // create a 2nd user with the same lastname as user

        val user2 = User().apply { id = 1; lastname = user.lastname }

        // create a 3rd user as control group
        val user3 = User().apply { id = 2; lastname = "no-positive-match" }

        repository.saveAll(listOf(user, user2, user3))

        assertThat(repository.removeByLastname(user.lastname)).isEqualTo(2L)
        assertThat(repository.existsById(user3.id)).isTrue()
    }

    @Test
    fun useSliceToLoadContent() {
        val totalNumberUsers = 11
        val source: MutableList<User> = ArrayList(totalNumberUsers)

        for (i in 1..totalNumberUsers) {
            val user = User().apply {
                id = i.toLong()
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
        val user0 = User().apply { id = 1; lastname = "lastname-0" }

        val user1 = User().apply { id = 2; lastname = "lastname-1" }

        val user2 = User().apply { id = 3; lastname = "lastname-2" }

        // we deliberately save the items in reverse
        repository.saveAll(listOf(user2, user1, user0))

        val result = repository.findFirst2ByOrderByLastnameAsc()

        assertThat(result).containsExactly(user0, user1)
    }

    @Test
    fun findByFirstnameOrLastnameUsingSpEL() {
        val first = User().apply { id = 1; lastname = "lastname" }

        val second = User().apply { id = 2; firstname = "firstname" }

        val third = User()

        repository.saveAll(listOf(first, second, third))

        val reference = User().apply { id = 3; firstname = "firstname"; lastname = "lastname" }

        val users = repository.findByFirstnameOrLastname(reference)

        assertThat(users).contains(first)
        assertThat(users).contains(second)
        assertThat(users).hasSize(2)
    }

    /**
     * Streaming data from the store by using a repository method that returns a [Stream]. Note, that since the
     * resulting [Stream] contains state it needs to be closed explicitly after use!
     */
    @Test
    fun useJava8StreamsWithCustomQuery() {
        val user1 = repository.save(User().apply { id = 1; firstname = "Customer1"; lastname = "Foo" })
        val user2 = repository.save(User().apply { id = 2; firstname = "Customer2"; lastname = "Bar" })

        repository.streamAllCustomers().use { stream ->
            assertThat(stream.collect(Collectors.toList())).contains(user1, user2)
        }
    }

    /**
     * Streaming data from the store by using a repository method that returns a [Stream] with a derived query.
     * Note, that since the resulting [Stream] contains state it needs to be closed explicitly after use!
     */
    @Test
    fun useJava8StreamsWithDerivedQuery() {
        val user1 = repository.save(User().apply { id = 1; firstname = "Customer1"; lastname = "Foo" })
        val user2 = repository.save(User().apply { id = 2; firstname = "Customer2"; lastname = "Bar" })

        repository.findAllByLastnameIsNotNull().use { stream ->
            assertThat(stream.collect(Collectors.toList())).contains(user1, user2)
        }
    }

    /**
     * Query methods using streaming need to be used inside a surrounding transaction to keep the connection open while
     * the stream is consumed. We simulate that not being the case by actively disabling the transaction here.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun rejectsStreamExecutionIfNoSurroundingTransactionActive() {
        Assertions.assertThrows(InvalidDataAccessApiUsageException::class.java) {
            repository.findAllByLastnameIsNotNull()
        }
    }

    /**
     * Here we demonstrate the usage of [CompletableFuture] as a result wrapper for asynchronous repository query
     * methods. Note, that we need to disable the surrounding transaction to be able to asynchronously read the written
     * data from another thread within the same test method.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun supportsCompletableFuturesAsReturnTypeWrapper() {
        repository.save(User().apply { id = 1; firstname = "Customer1"; lastname = "Foo" })
        repository.save(User().apply { id = 2; firstname = "Customer2"; lastname = "Bar" })

        runBlocking {
            val users = repository.readAllBy().await()
            assertThat(users).hasSize(2)
        }

        repository.deleteAll()
    }
}