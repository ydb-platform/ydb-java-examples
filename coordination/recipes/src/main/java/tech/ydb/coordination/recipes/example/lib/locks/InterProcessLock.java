package tech.ydb.coordination.recipes.example.lib.locks;

import java.time.Duration;

import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.recipes.example.lib.util.Listenable;

public interface InterProcessLock extends Listenable<CoordinationSession.State> {
    void acquire() throws Exception, LockAlreadyAcquiredException, LockAcquireFailedException;

    /**
     * @return true - if successfully acquired lock, false - if lock waiting time expired
     */
    boolean acquire(Duration waitDuration) throws Exception, LockAlreadyAcquiredException, LockAcquireFailedException;

    /**
     * @return false if nothing to release
     */
    boolean release() throws Exception;

    /**
     * @return true if the lock is acquired by a thread in this JVM
     */
    boolean isAcquiredInThisProcess();
}
