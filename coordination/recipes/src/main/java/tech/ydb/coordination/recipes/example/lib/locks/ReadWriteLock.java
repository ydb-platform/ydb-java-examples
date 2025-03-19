package tech.ydb.coordination.recipes.example.lib.locks;

import java.time.Duration;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.recipes.example.lib.util.Listenable;
import tech.ydb.coordination.recipes.example.lib.util.ListenableProvider;

public class ReadWriteLock {
    private final InternalLock readLock;
    private final InternalLock writeLock;

    public ReadWriteLock(
            CoordinationClient client,
            String coordinationNodePath,
            String lockName
    ) {
        LockInternals lockInternals = new LockInternals(
                client, coordinationNodePath, lockName
        );
        lockInternals.start();
        // TODO: Share same lockInternals?
        this.readLock = new InternalLock(lockInternals, false);
        this.writeLock = new InternalLock(lockInternals, true);
    }

    public InterProcessLock writeLock() {
        return readLock;
    }

    public InterProcessLock readLock() {
        return writeLock;
    }

    private static class InternalLock implements InterProcessLock, ListenableProvider<CoordinationSession.State> {
        private final LockInternals lockInternals;
        private final boolean isExclisive;

        private InternalLock(LockInternals lockInternals, boolean isExclisive) {
            this.lockInternals = lockInternals;
            this.isExclisive = isExclisive;
        }

        @Override
        public void acquire() throws Exception {
            lockInternals.tryAcquire(
                    null,
                    isExclisive,
                    null
            );
        }

        @Override
        public boolean acquire(Duration waitDuration) throws Exception {
            return lockInternals.tryAcquire(
                    waitDuration,
                    isExclisive,
                    null
            );
        }

        @Override
        public boolean release() {
            return lockInternals.release();
        }

        @Override
        public boolean isAcquiredInThisProcess() {
            return lockInternals.getProcessLease() != null;
        }

        @Override
        public Listenable<CoordinationSession.State> getListenable() {
            return lockInternals.getListenable();
        }
    }

}
