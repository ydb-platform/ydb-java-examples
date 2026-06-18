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

@Service
public class KvOperationService {
    private static final ThreadLocal<int[]> ATTEMPTS = ThreadLocal.withInitial(() -> new int[1]);

    public static void resetAttempts() {
        ATTEMPTS.get()[0] = 0;
    }

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

        entityManager.clear();
    }

    @Transactional
    public void executeDdl(String ddl) {
        entityManager.createNativeQuery(ddl).executeUpdate();
    }
}
