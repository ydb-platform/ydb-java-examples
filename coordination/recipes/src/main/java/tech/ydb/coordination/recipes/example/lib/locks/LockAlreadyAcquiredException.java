package tech.ydb.coordination.recipes.example.lib.locks;

public class LockAlreadyAcquiredException extends RuntimeException {
    private final String coordinationNodePath;
    private final String semaphoreName;

    public LockAlreadyAcquiredException(String coordinationNodePath, String semaphoreName) {
        super("Semaphore=" + semaphoreName + " on path=" + coordinationNodePath + " is already acquired");
        this.coordinationNodePath = coordinationNodePath;
        this.semaphoreName = semaphoreName;
    }

    public String getCoordinationNodePath() {
        return coordinationNodePath;
    }

    public String getSemaphoreName() {
        return semaphoreName;
    }
}
