package tech.ydb.apps.repo;

import java.util.UUID;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import tech.ydb.apps.entity.Token;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface TokenRepository extends CrudRepository<Token, UUID> {

    @Query(value = "SCAN SELECT id FROM app_token", nativeQuery = true)
    Iterable<String> scanFindAll();

    void saveAllAndFlush(Iterable<Token> list);

    void deleteAllByIdInBatch(Iterable<UUID> ids);
}
