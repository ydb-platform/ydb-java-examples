package tech.ydb.slo.springjpa;

import java.sql.Timestamp;
import java.util.List;

import jakarta.persistence.EntityManager;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.hibernate.jpa.AvailableHints;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import tech.ydb.retry.YdbTransactional;
import tech.ydb.slo.core.kv.KvSchema;
import tech.ydb.slo.core.kv.Row;

/**
 * KV operations executed inside Spring-managed JPA transactions with YDB retry.
 *
 * <p>Native queries are used because the workload table name is chosen at
 * runtime from the SLO action inputs.
 *
 * <p>The methods that record SLO latency take care to keep the Hibernate
 * persistence context out of the hot path: reads use {@code FlushMode.COMMIT}
 * and the {@code READ_ONLY} hint so Hibernate skips the dirty-check snapshot,
 * and writes call {@code clear()} after each upsert so per-worker entity-state
 * memory doesn't grow unbounded across thousands of iterations.
 *
 * <p>{@code spring-ydb-retry}'s {@code @YdbTransactional} wraps each call in an
 * AOP retry advice. The library does not expose a retry count, so the service
 * carries a thread-local counter that the AOP-wrapped methods increment on each
 * invocation; the caller resets it before calling and reads it after to
 * populate {@link tech.ydb.slo.core.kv.OpOutcome#retryAttempts()}.
 */
@Service
public class KvOperationService {

    private static final ThreadLocal<int[]> ATTEMPTS = ThreadLocal.withInitial(() -> new int[1]);

    /** Resets the attempt counter for the current thread. Call before each op. */
    public static void resetAttempts() {
        ATTEMPTS.get()[0] = 0;
    }

    /** Returns the number of attempts made on the current thread since the last reset. */
    public static int currentAttempts() {
        return ATTEMPTS.get()[0];
    }

    @PersistenceContext
    private EntityManager entityManager;

    @YdbTransactional(readOnly = true)
    public void read(String tablePath, long id, long hash) {
        ATTEMPTS.get()[0]++;
        Query query = entityManager.createNativeQuery(String.format(KvSchema.SELECT_TEMPLATE, tablePath))
                .setParameter(1, id)
                .setParameter(2, hash)
                .setHint(AvailableHints.HINT_READ_ONLY, true);
        query.setFlushMode(FlushModeType.COMMIT);
        @SuppressWarnings("unchecked")
        List<Object[]> rows = query.getResultList();
        for (Object[] row : rows) {
            if (row[0] != null) {
                ((Number) row[0]).longValue();
            }
        }
    }

    @YdbTransactional(idempotent = true)
    public void write(String tablePath, Row row, long hash) {
        ATTEMPTS.get()[0]++;
        Query query = entityManager.createNativeQuery(String.format(KvSchema.UPSERT_TEMPLATE, tablePath))
                .setParameter(1, hash)
                .setParameter(2, row.id())
                .setParameter(3, row.payloadStr())
                .setParameter(4, row.payloadDouble())
                .setParameter(5, Timestamp.from(row.payloadTimestamp()))
                .setParameter(6, row.payloadHash());
        query.setFlushMode(FlushModeType.COMMIT);
        query.executeUpdate();
        // Drop any managed state so the persistence context doesn't grow
        // across thousands of upserts on the same worker thread.
        entityManager.clear();
    }

    @Transactional
    public void executeDdl(String ddl) {
        entityManager.createNativeQuery(ddl).executeUpdate();
    }
}
