package tech.ydb.slo.kv;

import java.time.Instant;

/**
 * A single row of the KV workload table.
 *
 * <p>The schema mirrors the one used by SLO workloads in other YDB SDKs
 * (Go, JavaScript):
 * <pre>
 * hash              Uint64 (primary key, derived from id)
 * id                Uint64 (primary key)
 * payload_str       Utf8
 * payload_double    Double
 * payload_timestamp Timestamp
 * payload_hash      Uint64
 * </pre>
 *
 * <p>The {@code hash} primary-key column is derived from {@code id}; the JDBC
 * workload computes it on the client (see {@code KvWorkload}) so reads and
 * writes target the same key without relying on server-side YQL builtins in
 * parameterized statements.
 */
public final class Row {
    private final long id;
    private final String payloadStr;
    private final double payloadDouble;
    private final Instant payloadTimestamp;
    private final long payloadHash;

    public Row(
            long id,
            String payloadStr,
            double payloadDouble,
            Instant payloadTimestamp,
            long payloadHash
    ) {
        this.id = id;
        this.payloadStr = payloadStr;
        this.payloadDouble = payloadDouble;
        this.payloadTimestamp = payloadTimestamp;
        this.payloadHash = payloadHash;
    }

    public long id() {
        return id;
    }

    public String payloadStr() {
        return payloadStr;
    }

    public double payloadDouble() {
        return payloadDouble;
    }

    public Instant payloadTimestamp() {
        return payloadTimestamp;
    }

    public long payloadHash() {
        return payloadHash;
    }
}
