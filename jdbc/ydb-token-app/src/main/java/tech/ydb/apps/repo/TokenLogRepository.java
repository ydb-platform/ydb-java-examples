package tech.ydb.apps.repo;

import java.util.Optional;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import tech.ydb.apps.entity.TokenLog;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface TokenLogRepository extends CrudRepository<TokenLog, String> {
    @Query("SELECT MAX(log.globalVersion) FROM TokenLog log")
    Optional<Long> findTopGlobalVersion();
}
