package tech.ydb.slo.springjpa;

import java.sql.Timestamp;
import java.util.List;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import tech.ydb.retry.YdbTransactional;
import tech.ydb.slo.core.kv.Row;

/**
 * KV operations executed inside Spring-managed JPA transactions with YDB retry.
 *
 * <p>Native queries are used because the workload table name is chosen at
 * runtime from the SLO action inputs.
 */
@Service
public class KvOperationService {
    @PersistenceContext
    private EntityManager entityManager;

    @YdbTransactional(readOnly = true)
    public void read(String tablePath, long id, long hash) {
        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager.createNativeQuery(
                "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
                        + "  FROM `" + tablePath + "`"
                        + "  WHERE id = ? AND hash = ?"
        )
                .setParameter(1, id)
                .setParameter(2, hash)
                .getResultList();
        for (Object[] row : rows) {
            if (row[0] != null) {
                ((Number) row[0]).longValue();
            }
        }
    }

    @YdbTransactional(idempotent = true)
    public void write(String tablePath, Row row, long hash) {
        entityManager.createNativeQuery(
                "UPSERT INTO `" + tablePath + "` ("
                        + "  hash, id, payload_str, payload_double, payload_timestamp, payload_hash"
                        + ") VALUES (?, ?, ?, ?, ?, ?)"
        )
                .setParameter(1, hash)
                .setParameter(2, row.id())
                .setParameter(3, row.payloadStr())
                .setParameter(4, row.payloadDouble())
                .setParameter(5, Timestamp.from(row.payloadTimestamp()))
                .setParameter(6, row.payloadHash())
                .executeUpdate();
    }

    @Transactional
    public void executeDdl(String ddl) {
        entityManager.createNativeQuery(ddl).executeUpdate();
    }
}
