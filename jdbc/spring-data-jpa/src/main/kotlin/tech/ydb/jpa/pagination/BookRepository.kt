package tech.ydb.jpa.pagination

import org.springframework.data.domain.*
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.ListCrudRepository
import org.springframework.data.repository.query.Param

interface BookRepository : ListCrudRepository<Book, String> {

    /**
     * Uses an offset based pagination that first sorts the entries by their [ publication_date][Book.getPublicationDate]
     * and then limits the result by dropping the number of rows specified in the
     * [offset][Pageable.getOffset] clause. To retrieve [Page.getTotalElements] an additional count query
     * is executed.
     *
     * @param title
     * @param pageable
     */
    @Query(
            value = "SELECT * FROM books WHERE title LIKE %:title% ORDER BY books.publication_date",
            countQuery = "SELECT count(*) FROM books WHERE title LIKE %:title% ",
            nativeQuery = true
    )
    fun findByTitleContainsOrderByPublicationDate(@Param("title") title: String, pageable: Pageable): Page<Book>

    /**
     * Uses an offset based slicing that first sorts the entries by their [ publication_date][Book.getPublicationDate] and then limits the result by dropping the number of rows specified in the
     * [offset][Pageable.getOffset] clause.
     *
     * @param title
     * @param pageable
     */
    @Query(
            value = "SELECT * FROM books WHERE title LIKE %:title% ORDER BY books.publication_date",
            nativeQuery = true
    )
    fun findBooksByTitleContainsOrderByPublicationDate(title: String, pageable: Pageable): Slice<Book>

    /**
     * Depending on the provided [ScrollPosition] either [ offset][org.springframework.data.domain.OffsetScrollPosition]
     * or [keyset][org.springframework.data.domain.KeysetScrollPosition] scrolling is possible. Scrolling
     * through results requires a stable [org.springframework.data.domain.Sort] which is different from what
     * [Pageable.getSort] offers. The limit is defined via the Top keyword.
     *
     * @param title
     * @param scrollPosition
     */
    fun findTop2ByTitleContainsOrderByPublicationDate(title: String, scrollPosition: ScrollPosition): Window<Book>
}
