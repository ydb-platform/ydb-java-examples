package tech.ydb.slo.springjdbc;

import java.sql.Timestamp;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

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
