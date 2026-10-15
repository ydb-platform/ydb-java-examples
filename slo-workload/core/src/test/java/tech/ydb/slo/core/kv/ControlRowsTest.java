package tech.ydb.slo.core.kv;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ControlRowsTest {
    private final Row expected = new Row(Long.MAX_VALUE, "control/7", 7.25, Instant.ofEpochSecond(1, 123000), 7);

    @Test
    void exactRowsOnly() {
        assertThrows(IllegalStateException.class, () -> RowGenerator.generate(Long.MAX_VALUE));
        assertDoesNotThrow(() -> ControlRows.requireRow(OpOutcome.success(3, List.of(expected)), expected));
        assertThrows(IllegalStateException.class, () -> ControlRows.requireRow(OpOutcome.success(0), expected));
        assertThrows(IllegalStateException.class, () -> ControlRows.requireRow(
                OpOutcome.success(0, List.of(expected, expected)), expected));
        for (Row bad : List.of(
                new Row(1, expected.payloadStr(), 7.25, expected.payloadTimestamp(), 7),
                new Row(expected.id(), expected.payloadStr(), 7.25, expected.payloadTimestamp(), 6),
                new Row(expected.id(), "corrupt", 7.25, expected.payloadTimestamp(), 7),
                new Row(expected.id(), expected.payloadStr(), 8.25, expected.payloadTimestamp(), 7),
                new Row(expected.id(), expected.payloadStr(), 7.25, expected.payloadTimestamp().plusNanos(1), 7))) {
            assertThrows(IllegalStateException.class, () -> ControlRows.requireRow(
                    OpOutcome.success(0, List.of(bad)), expected));
        }
    }

    @Test
    void preservesSdkFailureBeforeCheckingPresence() {
        Throwable cause = new RuntimeException("sdk");
        assertSame(cause, assertThrows(IllegalStateException.class, () -> ControlRows.requireRow(
                OpOutcome.error(2, "sdk", cause), expected)).getCause());
    }

    @Test
    void sequentialOwnershipAndRetainedGeneration() {
        ControlRows control = new ControlRows();
        KvWorkloadParams params = new KvWorkloadParams();
        class Session implements KvSession {
            Row stored;
            int writes;
            int reads;
            public OpOutcome write(Row row, int timeoutMs) {
                assertEquals(writes++, row.payloadHash());
                assertEquals(0, row.payloadTimestamp().getNano() % 1000);
                stored = row;
                return OpOutcome.success(2);
            }
            public OpOutcome read(long id, int timeoutMs) {
                reads++;
                assertEquals(stored.id(), id);
                return OpOutcome.success(1, List.of(stored));
            }
        }
        Session session = new Session();
        control.verify(session, params);
        assertEquals(1, session.writes);
        control.verify(session, params);
        assertEquals(2, session.writes);
        assertEquals(3, session.reads);
        session.stored = expected;
        assertThrows(IllegalStateException.class, () -> control.verify(session, params));
        assertEquals(2, session.writes);
    }

    @Test
    void failedWriteDoesNotPublishReadableRow() {
        int[] reads = {0};
        Throwable cause = new RuntimeException("write");
        KvSession session = new KvSession() {
            public OpOutcome write(Row row, int timeoutMs) { return OpOutcome.error(1, "sdk", cause); }
            public OpOutcome read(long id, int timeoutMs) { reads[0]++; return OpOutcome.success(0); }
        };
        assertSame(cause, assertThrows(IllegalStateException.class,
                () -> new ControlRows().verify(session, new KvWorkloadParams())).getCause());
        assertEquals(0, reads[0]);
    }
}
