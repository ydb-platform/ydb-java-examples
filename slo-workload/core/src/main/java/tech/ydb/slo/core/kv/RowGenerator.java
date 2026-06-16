package tech.ydb.slo.core.kv;

import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates rows for the KV workload.
 *
 * <p>Each row gets a monotonically increasing {@code id} and a random payload.
 * The {@code hash} primary-key column is derived from {@code id} by the
 * workload at insert time, so it is not carried on the row. The format mirrors
 * the SLO workloads in the Go and JS SDKs so the resulting tables are
 * interchangeable.
 */
public final class RowGenerator {
    private static final int MIN_PAYLOAD_LENGTH = 20;
    private static final int MAX_PAYLOAD_LENGTH = 40;

    private final AtomicLong nextId;

    public RowGenerator(long startId) {
        this.nextId = new AtomicLong(startId);
    }

    /**
     * Generates a new row with a fresh monotonically increasing id.
     * @return a new row
     */
    public Row generate() {
        long id = nextId.getAndIncrement();
        return generate(id);
    }

    /**
     * Generates a row with an explicit id (used during prefill to control IDs).
     * @param id row id
     * @return a new row
     */
    public static Row generate(long id) {
        long payloadHash = ThreadLocalRandom.current().nextLong();
        double payloadDouble = ThreadLocalRandom.current().nextDouble();
        String payloadStr = randomPayloadString();
        Instant payloadTimestamp = Instant.now();

        return new Row(id, payloadStr, payloadDouble, payloadTimestamp, payloadHash);
    }

    private static String randomPayloadString() {
        int length = MIN_PAYLOAD_LENGTH
                + ThreadLocalRandom.current().nextInt(MAX_PAYLOAD_LENGTH - MIN_PAYLOAD_LENGTH + 1);
        byte[] bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(bytes);
        return Base64.getEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Derives the primary-key {@code hash} column from {@code id} using a
     * SplitMix64-style mix. Workloads that compute the hash on the client
     * (JDBC, Spring Data) use this so reads and writes always target the same
     * key. The exact function does not need to match YQL's
     * {@code Digest::NumericHash}; it only needs to be deterministic and well
     * distributed across partitions.
     * @param id row id
     * @return derived hash value
     */
    public static long numericHash(long id) {
        long z = id + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
