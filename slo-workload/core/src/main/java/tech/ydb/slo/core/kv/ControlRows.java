package tech.ydb.slo.core.kv;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** One runner owns this key in its process-private table; load IDs start at zero. */
final class ControlRows {
    private Row published;
    private long generation;

    void verify(KvSession session, KvWorkloadParams params) {
        if (published != null) {
            requireRow(session.read(published.id(), params.readTimeoutMs()), published);
        }
        Row next = new Row(Long.MAX_VALUE, "control/" + generation, generation + 0.25,
                Instant.now().truncatedTo(ChronoUnit.MICROS), generation);
        requireSuccess(session.write(next, params.writeTimeoutMs()));
        published = next;
        requireRow(session.read(next.id(), params.readTimeoutMs()), next);
        generation++;
    }

    private static void requireSuccess(OpOutcome outcome) {
        if (!outcome.isSuccess()) {
            throw new IllegalStateException("Control operation failed: " + outcome.errorKind(), outcome.cause());
        }
    }

    static void requireRow(OpOutcome outcome, Row expected) {
        requireSuccess(outcome);
        if (outcome.rows().size() != 1) {
            throw new IllegalStateException("Control row cardinality: " + outcome.rows().size());
        }
        Row actual = outcome.rows().get(0);
        if (actual.id() != expected.id() || actual.payloadHash() != expected.payloadHash()
                || !expected.payloadStr().equals(actual.payloadStr())
                || Double.compare(expected.payloadDouble(), actual.payloadDouble()) != 0
                || !expected.payloadTimestamp().equals(actual.payloadTimestamp())) {
            throw new IllegalStateException("Control row mismatch at generation " + expected.payloadHash());
        }
    }
}
