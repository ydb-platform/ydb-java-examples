package tech.ydb.slo.springjdbc;

import java.sql.Timestamp;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import tech.ydb.retry.YdbTransactional;
import tech.ydb.slo.core.kv.Row;

/**
 * KV operations executed inside Spring-managed transactions with YDB retry.
 */
@Service
public class KvOperationService {
    private final JdbcTemplate jdbc;

    public KvOperationService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @YdbTransactional(readOnly = true)
    public void read(String tablePath, long id, long hash) {
        String sql = ""
                + "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
                + "  FROM `" + tablePath + "`"
                + "  WHERE id = ? AND hash = ?";
        jdbc.query(sql, rs -> {
            while (rs.next()) {
                rs.getLong("id");
            }
        }, id, hash);
    }

    @YdbTransactional(idempotent = true)
    public void write(String tablePath, Row row, long hash) {
        String sql = ""
                + "UPSERT INTO `" + tablePath + "` ("
                + "  hash, id, payload_str, payload_double, payload_timestamp, payload_hash"
                + ") VALUES (?, ?, ?, ?, ?, ?)";
        jdbc.update(
                sql,
                hash,
                row.id(),
                row.payloadStr(),
                row.payloadDouble(),
                Timestamp.from(row.payloadTimestamp()),
                row.payloadHash()
        );
    }
}
