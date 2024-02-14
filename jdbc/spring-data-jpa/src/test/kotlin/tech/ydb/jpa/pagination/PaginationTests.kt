package tech.ydb.jpa.pagination

import com.github.javafaker.Faker
import jakarta.persistence.EntityManager
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.domain.Page
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Window
import org.springframework.data.domain.ScrollPosition
import org.springframework.data.domain.Slice
import org.springframework.data.support.WindowIterator
import org.springframework.transaction.annotation.Transactional
import tech.ydb.jpa.PostgresDockerTest
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Show different types of paging styles using [Page], [Slice] and [Window].
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@SpringBootTest
@Transactional
internal class PaginationTests : PostgresDockerTest() /* TODO ESCAPE '\' don't support */{

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

    /**
     * Scroll through the results using an offset/limit approach where the server skips over the number of results
     * specified via [OffsetScrollPosition.getOffset].
     *
     *
     * This approach is similar to the [slicing one][.sliceThroughResultsWithSkipAndLimit].
     */
    @Test
    fun scrollThroughResultsWithSkipAndLimit() {
        var window: Window<Book>
        var scrollPosition: ScrollPosition = ScrollPosition.offset()

        do {
            window = books.findTop2ByTitleContainsOrderByPublicationDate("the", scrollPosition)
            assertThat(window.content.size).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(2)

            scrollPosition = window.positionAt(window.content.size - 1)
        } while (window.hasNext())
    }

    /**
     * Scroll through the results using an offset/limit approach where the server skips over the number of results
     * specified via [OffsetScrollPosition.getOffset] using [WindowIterator].
     *
     *
     * This approach is similar to the [slicing one][.sliceThroughResultsWithSkipAndLimit].
     */
    @Test
    fun scrollThroughResultsUsingWindowIteratorWithSkipAndLimit() {
        val iterator: WindowIterator<Book> = WindowIterator
                .of { scrollPosition: ScrollPosition -> books.findTop2ByTitleContainsOrderByPublicationDate("the-crazy-book-", scrollPosition) }
                .startingAt(ScrollPosition.offset())

        val allBooks: List<Book> = iterator.asSequence().toList()

        assertThat(allBooks).hasSize(50)
    }

    /**
     * Scroll through the results using an index based approach where the [keyset][KeysetScrollPosition.getKeys]
     * keeps track of already seen values to resume scrolling by altering the where clause to only return rows after the
     * values contained in the keyset. Set logging.level.org.hibernate.SQL=debug to show the modified query in
     * the log.
     */
    @Test
    fun scrollThroughResultsWithKeyset() {
        var window: Window<Book>
        var scrollPosition: ScrollPosition = ScrollPosition.keyset()
        do {
            window = books.findTop2ByTitleContainsOrderByPublicationDate("the", scrollPosition)
            assertThat(window.content.size).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(2)

            scrollPosition = window.positionAt(window.content.size - 1)
        } while (window.hasNext())
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
