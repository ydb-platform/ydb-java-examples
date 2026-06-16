package tech.ydb.slo.springjdbc;

import java.sql.Timestamp;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import tech.ydb.retry.YdbTransactional;
import tech.ydb.slo.core.kv.KvSchema;
import tech.ydb.slo.core.kv.Row;

/**
 * KV operations executed inside Spring-managed transactions with YDB retry.
 *
 * <p>{@code spring-ydb-retry}'s {@code @YdbTransactional} wraps each call in an
 * AOP retry advice. The library does not expose a retry count, so the service
 * carries a thread-local counter that the AOP-wrapped methods increment on each
 * invocation; the caller resets it before calling and reads it after to
 * populate {@link tech.ydb.slo.core.kv.OpOutcome#retryAttempts()}. Without
 * this, the workload's {@code sdk.retry.attempts.total} series would be flat
 * for Spring-backed workloads and incomparable to the JDBC and query
 * implementations.
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

    private final JdbcTemplate jdbc;

    public KvOperationService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @YdbTransactional(readOnly = true)
    public void read(String tablePath, long id, long hash) {
        ATTEMPTS.get()[0]++;
        jdbc.query(String.format(KvSchema.SELECT_TEMPLATE, tablePath), rs -> {
            while (rs.next()) {
                rs.getLong("id");
            }
        }, id, hash);
    }

    @YdbTransactional(idempotent = true)
    public void write(String tablePath, Row row, long hash) {
        ATTEMPTS.get()[0]++;
        jdbc.update(
                String.format(KvSchema.UPSERT_TEMPLATE, tablePath),
                hash,
                row.id(),
                row.payloadStr(),
                row.payloadDouble(),
                Timestamp.from(row.payloadTimestamp()),
                row.payloadHash()
        );
    }
}
