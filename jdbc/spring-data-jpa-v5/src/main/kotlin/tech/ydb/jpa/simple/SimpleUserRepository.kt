package tech.ydb.jpa.simple;

import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.scheduling.annotation.Async
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream

/**
 * Simple repository interface for {@link User} instances. The interface is used to declare the so-called query methods,
 * i.e. methods to retrieve single entities or collections of them.
 */
interface SimpleUserRepository : CrudRepository<User, Long> {

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
	fun findByUsername(username: String?): User?

	/**
	 * Find all users with the given lastname. This method will be translated into a query by constructing it directly
	 * from the method name as there is no other query declared.
	 *
	 * @param lastname
	 */
	 fun findByLastname(lastname: String): List<User>


	/**
	 * Returns all users with the given firstname. This method will be translated into a query using the one declared in
	 * the {@link Query} annotation declared one.
	 *
	 * @param firstname
	 */
	@Query("select u from User u where u.firstname = :firstname")
	fun findByFirstname(firstname: String): List<User>


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
	@Query(
			"SELECT * FROM users WHERE users.lastname = :lastname ORDER BY users.username",
			nativeQuery = true
	)
	fun findByLastnameOrderByUsernameAsc(lastname: String, page: Pageable): Slice<User>

	/**
	 * Return the first 2 users ordered by their lastname asc.
	 *
	 * <pre>
	 * Example for findFirstK / findTopK functionality.
	 * </pre>
	 */
	@Query("SELECT * FROM users ORDER BY lastname LIMIT 2", nativeQuery = true)
	fun findFirst2ByOrderByLastnameAsc(): List<User>


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
