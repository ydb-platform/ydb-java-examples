package tech.ydb.jpa.simple;

import jakarta.persistence.QueryHint
import org.hibernate.jpa.HibernateHints
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.scheduling.annotation.Async;
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream

/**
 * Simple repository interface for {@link User} instances. The interface is used to declare the so-called query methods,
 * i.e. methods to retrieve single entities or collections of them.
 */
interface SimpleUserRepository : ListCrudRepository<User, Long> {

    /**
     * Find the user with the given username. This method will be translated into a query using the
     * {@link jakarta.persistence.NamedQuery} annotation at the {@link User} class.
     *
     * @param username
     */
    fun findByTheUsersName(username: String): User

    /**
     * Uses {@link Optional} as return and parameter type.
     *
     * @param username
     */
    @QueryHints(value = [QueryHint(name = HibernateHints.HINT_COMMENT, value = "use_index:username_index")])
    fun findByUsername(username: String?): User?

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
     * Returns all users with the given firstname. This method will be translated into a query using the one declared in
     * the {@link Query} annotation declared one.
     *
     * @param firstname
     */
    @Query("select u from User u where u.firstname = :firstname")
    fun findByFirstname(firstname: String): List<User>

    /**
     * Returns at most the number of users defined via {@link Limit} with the given firstname. This method will be
     * translated into a query using the one declared in the {@link Query} annotation declared one.
     *
     * @param firstname
     * @param maxResults the maximum number of results returned.
     */
    @Query("select u from User u where u.firstname = :firstname")
    fun findByFirstname(firstname: String, maxResults: Limit): List<User>

    /**
     * Returns all users with the given name as first- or lastname. This makes the query to method relation much more
     * refactoring-safe as the order of the method parameters is completely irrelevant.
     *
     * @param name
     */
    @Query("select u from User u where u.firstname = :name or u.lastname = :name")
    fun findByFirstnameOrLastname(name: String): List<User>

    /**
     * Returns the total number of entries deleted as their lastnames match the given one.
     *
     * @param lastname
     * @return
     */
    fun removeByLastname(lastname: String): Long

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
    @Query("select u from User u where u.firstname = :#{#user.firstname} or u.lastname = :#{#user.lastname}")
    fun findByFirstnameOrLastname(user: User): Iterable<User>

    /**
     * Sample default method.
     *
     * @param user
     */
    fun findByLastname(user: User): List<User> {
        return findByLastname(user.lastname);
    }

    /**
     * Sample method to demonstrate support for {@link Stream} as a return type with a custom query. The query is executed
     * in a streaming fashion which means that the method returns as soon as the first results are ready.
     */
    @Query("select u from User u")
    fun streamAllCustomers(): Stream<User>

    /**
     * Sample method to demonstrate support for {@link Stream} as a return type with a derived query. The query is
     * executed in a streaming fashion which means that the method returns as soon as the first results are ready.
     */
    fun findAllByLastnameIsNotNull(): Stream<User>

    @Async
    fun readAllBy(): CompletableFuture<List<User>>
}
