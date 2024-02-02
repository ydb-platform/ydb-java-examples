package tech.ydb.jpa.pagination

import com.github.javafaker.Faker
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.domain.Page
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.transaction.annotation.Transactional
import tech.ydb.jpa.PostgresDockerTest
import tech.ydb.jpa.YdbDockerTest
import java.util.*
import java.util.concurrent.TimeUnit
import javax.persistence.EntityManager

/**
 * Show different types of paging styles using [Page], [Slice] and [Window].
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@SpringBootTest
@Transactional
internal class PaginationTests : YdbDockerTest() {

    @Autowired
    lateinit var books: BookRepository

    @BeforeEach
    fun setUp() {
        val faker = Faker()

        val authorList = createAuthors(faker)
        createBooks(faker, authorList)
    }

    /**
     * Page through the results using an offset/limit approach where the server skips over the number of results specified
     * via [Pageable.getOffset]. The [Page] return type will run an additional count query to
     * read the total number of matching rows on each request.
     */
    @Test
    fun pageThroughResultsWithSkipAndLimit() {
        var page: Page<Book>
        var pageRequest: Pageable = PageRequest.of(0, 2)

        do {
            page = books.findByTitleContainsOrderByPublicationDate("the", pageRequest)
            assertThat(page.content.size).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(2)

            pageRequest = page.nextPageable()
        } while (page.hasNext())
    }

    /**
     * Run through the results using an offset/limit approach where the server skips over the number of results specified
     * via [Pageable.getOffset]. No additional count query to read the total number of matching rows is
     * issued. Still [Slice] requests, but does not emit, one row more than specified via [Page.getSize] to
     * feed [Slice.hasNext]
     */
    @Test
    fun sliceThroughResultsWithSkipAndLimit() {
        var slice: Slice<Book>
        var pageRequest: Pageable = PageRequest.of(0, 2)

        do {
            slice = books.findBooksByTitleContainsOrderByPublicationDate("the", pageRequest)
            assertThat(slice.content.size).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(2)

            pageRequest = slice.nextPageable()
        } while (slice.hasNext())
    }

    // --> Test Data
    @Autowired
    lateinit var em: EntityManager

    private fun createAuthors(faker: Faker): List<Author> {
        val authors = List(11) { id: Int ->
            val author = Author()
            author.id = "author-%s".format(id)
            author.firstName = faker.name().firstName()
            author.lastName = faker.name().lastName()

            em.persist(author)
            author
        }

        return authors
    }

    private fun createBooks(faker: Faker, authors: List<Author>): List<Book> {
        val rand = Random()
        return List(100) { id: Int ->
            val book = Book()
            book.id = "book-%03d".format(id)
            book.title = (if (id % 2 == 0) "the-crazy-book-" else "") + faker.book().title()
            book.isbn10 = UUID.randomUUID().toString().substring(0, 10)
            book.publicationDate = faker.date().past(5000, TimeUnit.DAYS)
            book.author = authors[rand.nextInt(authors.size)]

            em.persist(book)
            book
        }
    }
}
