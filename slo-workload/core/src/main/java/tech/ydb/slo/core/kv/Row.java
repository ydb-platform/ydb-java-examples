package tech.ydb.slo.core.kv;

import java.time.Instant;

/**
 * A single row of the KV workload table.
 *
 * <p>The schema mirrors the one used by SLO workloads in other YDB SDKs
 * (Go, JavaScript) so reports across SDKs are comparable:
 * <pre>
 * hash              Uint64 (primary key, derived from id)
 * id                Uint64 (primary key)
 * payload_str       Utf8
 * payload_double    Double
 * payload_timestamp Timestamp
 * payload_hash      Uint64
 * </pre>
 *
 * <p>The {@code hash} primary-key column is derived from {@code id} by each
 * workload (either server-side via {@code Digest::NumericHash($id)} or
 * client-side), so it is not carried on the row.
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
