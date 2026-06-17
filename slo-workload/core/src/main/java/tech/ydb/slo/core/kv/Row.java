package tech.ydb.slo.core.kv;

import java.time.Instant;

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
