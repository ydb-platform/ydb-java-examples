package tech.ydb.jdbc.simple

import org.springframework.data.domain.Limit
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.domain.Sort
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.repository.ListCrudRepository
import tech.ydb.data.repository.ViewIndex

/**
 * Simple repository interface for {@link User} instances. The interface is used to declare the so-called query methods,
 * i.e. methods to retrieve single entities or collections of them.
 */
interface SimpleUserRepository : ListCrudRepository<User, Long> {

    /**
     * Uses {@link Optional} as return and parameter type.
     *
     * @param username
     */
    @ViewIndex(indexName = "username_index")
    fun findByUsername(username: String): User?

    /**
     * Find all users with the given lastname. This method will be translated into a query by constructing it directly
     * from the method name as there is no other query declared.
     *
     * @param lastname
     */
    fun findByLastname(lastname: String): List<User>

    /**
     * Find at most the number of users defined via maxResults with the given lastname.
     * This method will be translated into a query by constructing it directly from the method name as there is no other
     * query declared.
     *
     * @param lastname
     * @param maxResults the maximum number of results returned.
     */

    fun findByLastname(lastname: String, maxResults: Limit): List<User>

    /**
     * Returns at most the number of users defined via {@link Limit} with the given firstname. This method will be
     * translated into a query using the one declared in the {@link Query} annotation declared one.
     *
     * @param firstname
     * @param maxResults the maximum number of results returned.
     */
    fun findByFirstname(firstname: String, maxResults: Limit): List<User>

    /**
     * Returns all users with the given name as first- or lastname. This makes the query to method relation much more
     * refactoring-safe as the order of the method parameters is completely irrelevant.
     *
     * @param name
     */
    @Query("select * from Users u where u.firstname = :name or u.lastname = :name")
    fun findByFirstnameOrLastname(name: String): List<User>

    /**
     * Returns a {@link Slice} counting a maximum number of {@link Pageable#getPageSize()} users matching given criteria
     * starting at {@link Pageable#getOffset()} without prior count of the total number of elements available.
     *
     * @param lastname
     * @param page
     */
    fun findByLastnameOrderByUsernameAsc(lastname: String, page: Pageable): Slice<User>

    /**
     * Return the first 2 users ordered by their lastname asc.
     *
     * <pre>
     * Example for findFirstK / findTopK functionality.
     * </pre>
     */
    fun findFirst2ByOrderByLastnameAsc(): List<User>

    /**
     * Return the first 2 users ordered by the given {@code sort} definition.
     *
     * <pre>
     * This variant is very flexible because one can ask for the first K results when a ASC ordering
     * is used as well as for the last K results when a DESC ordering is used.
     * </pre>
     *
     * @param sort
     */
    fun findTop2By(sort: Sort): List<User>

    /**
     * Return all the users with the given firstname or lastname. Makes use of SpEL (Spring Expression Language).
     *
     * @param user
     */
    @Query("select * from Users u where u.firstname = :#{#user.firstname} or u.lastname = :#{#user.lastname}")
    fun findByFirstnameOrLastname(user: User): Iterable<User>
}
