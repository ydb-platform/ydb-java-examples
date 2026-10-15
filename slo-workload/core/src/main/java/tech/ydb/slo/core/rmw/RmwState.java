package tech.ydb.slo.core.rmw;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The expected image is retained by the admission loop, never reconstructed from storage. */
public final class RmwState {
    private RmwState() { }

    public static final class Operation {
        private final String id;
        private final long counter;
        private final long delta;

        public Operation(String id, long counter, long delta) {
            this.id = id;
            this.counter = counter;
            this.delta = delta;
        }

        public String id() { return id; }
        public long counter() { return counter; }
        public long delta() { return delta; }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Operation)) {
                return false;
            }
            Operation that = (Operation) other;
            return id.equals(that.id) && counter == that.counter && delta == that.delta;
        }

        @Override
        public int hashCode() { return Objects.hash(id, counter, delta); }
    }

    public static final class Counter {
        private final long id;
        private final long value;
        private final long version;

        public Counter(long id, long value, long version) {
            this.id = id;
            this.value = value;
            this.version = version;
        }

        public long id() { return id; }
        public long value() { return value; }
        public long version() { return version; }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Counter)) {
                return false;
            }
            Counter that = (Counter) other;
            return id == that.id && value == that.value && version == that.version;
        }

        @Override
        public int hashCode() { return Objects.hash(id, value, version); }

        @Override
        public String toString() { return "Counter(" + id + ", " + value + ", " + version + ")"; }
    }

    public static final class Snapshot {
        private final List<Counter> counters;
        private final List<Operation> operations;

        public Snapshot(List<Counter> counters, List<Operation> operations) {
            this.counters = List.copyOf(counters);
            this.operations = List.copyOf(operations);
        }

        public List<Counter> counters() { return counters; }
        public List<Operation> operations() { return operations; }
    }

    public static void validate(Map<String, Operation> expected, Snapshot actual) {
        Map<String, Operation> ledger = new HashMap<>();
        for (Operation operation : actual.operations()) {
            if (ledger.put(operation.id(), operation) != null) {
                throw new IllegalStateException("Duplicate logical ID: " + operation.id());
            }
        }
        if (!expected.equals(ledger)) {
            throw new IllegalStateException("Ledger does not match admitted IDs and payloads");
        }

        long[] totals = new long[2];
        long[] versions = new long[2];
        for (Operation operation : expected.values()) {
            int counter = Math.toIntExact(operation.counter());
            totals[counter] = Math.addExact(totals[counter], operation.delta());
            versions[counter] = Math.incrementExact(versions[counter]);
        }
        Map<Long, Counter> counters = new HashMap<>();
        for (Counter counter : actual.counters()) {
            if (counters.put(counter.id(), counter) != null) {
                throw new IllegalStateException("Duplicate counter: " + counter.id());
            }
        }
        if (counters.size() != 2) {
            throw new IllegalStateException("Expected exactly two counters");
        }
        for (int id = 0; id < 2; id++) {
            Counter wanted = new Counter(id, totals[id], versions[id]);
            if (!wanted.equals(counters.get((long) id))) {
                throw new IllegalStateException("Counter differs from admitted operations: expected "
                        + wanted + ", got " + counters.get((long) id));
            }
        }
    }
}
