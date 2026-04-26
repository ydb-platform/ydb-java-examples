package tech.ydb.slo.kv;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates rows for the KV workload.
 *
 * <p>Each row gets a monotonically increasing {@code id} and a random payload.
 * The {@code hash} column is computed server-side via
 * {@code Digest::NumericHash($id)} at insert time, so it is not carried on
 * the client. The format mirrors the SLO workloads in the Go and JS SDKs so
 * the resulting tables are interchangeable.
 */
public final class RowGenerator {
    private static final int MIN_PAYLOAD_LENGTH = 20;
    private static final int MAX_PAYLOAD_LENGTH = 40;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final AtomicLong nextId;

    public RowGenerator(long startId) {
        this.nextId = new AtomicLong(startId);
    }

    /**
     * Generates a new row with a fresh monotonically increasing id.
     */
    public Row generate() {
        long id = nextId.getAndIncrement();
        return generate(id);
    }

    /**
     * Generates a row with an explicit id (used during prefill to control IDs).
     */
    public Row generate(long id) {
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
        SECURE_RANDOM.nextBytes(bytes);
        return Base64.getEncoder().withoutPadding().encodeToString(bytes);
    }
}
