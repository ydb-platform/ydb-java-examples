package tech.ydb.slo.core.kv;

import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public final class RowGenerator {
    private static final int MIN_PAYLOAD_LENGTH = 20;
    private static final int MAX_PAYLOAD_LENGTH = 40;

    private final AtomicLong nextId;

    public RowGenerator(long startId) {
        this.nextId = new AtomicLong(startId);
    }

    public Row generate() {
        long id = nextId.getAndIncrement();
        return generate(id);
    }

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

    public static long numericHash(long id) {
        long z = id + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
