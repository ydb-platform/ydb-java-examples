package tech.ydb.slo.core.rmw;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RmwStateTest {
    private final RmwState.Operation first = new RmwState.Operation("0:0", 0, 3);
    private final RmwState.Operation second = new RmwState.Operation("1:0", 1, 5);
    private final Map<String, RmwState.Operation> expected = Map.of(first.id(), first, second.id(), second);
    private final List<RmwState.Counter> correct = List.of(
            new RmwState.Counter(0, 3, 1), new RmwState.Counter(1, 5, 1));

    @Test
    void exactRowsPassIncludingEmptyRun() {
        assertDoesNotThrow(() -> RmwState.validate(expected,
                new RmwState.Snapshot(correct, List.of(second, first))));
        assertDoesNotThrow(() -> RmwState.validate(Map.of(), new RmwState.Snapshot(List.of(
                new RmwState.Counter(0, 0, 0), new RmwState.Counter(1, 0, 0)), List.of())));
    }

    @Test
    void sumPreservingCounterCorruptionFails() {
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(List.of(new RmwState.Counter(0, 2, 1),
                        new RmwState.Counter(1, 6, 1)), List.of(first, second))));
    }

    @Test
    void coherentlyMissingLedgerAndCounterFails() {
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(List.of(new RmwState.Counter(0, 0, 0), correct.get(1)), List.of(second))));
    }

    @Test
    void wrongVersionFailsIndependentlyOfValue() {
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(List.of(new RmwState.Counter(0, 3, 0), correct.get(1)),
                        List.of(first, second))));
    }

    @Test
    void wrongPayloadOrCounterFailsEvenWithSameIds() {
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(correct, List.of(new RmwState.Operation(first.id(), 0, 4), second))));
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(correct, List.of(new RmwState.Operation(first.id(), 1, 3), second))));
    }

    @Test
    void duplicateExtraAndMissingRowsFail() {
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(correct, List.of(first, first, second))));
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(correct, List.of(first, second, new RmwState.Operation("9:9", 0, 1)))));
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(List.of(correct.get(0)), List.of(first, second))));
        assertThrows(IllegalStateException.class, () -> RmwState.validate(expected,
                new RmwState.Snapshot(List.of(correct.get(0), correct.get(0)), List.of(first, second))));
    }
}
