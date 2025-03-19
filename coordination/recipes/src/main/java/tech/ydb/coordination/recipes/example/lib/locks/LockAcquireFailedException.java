package tech.ydb.coordination.recipes.example.lib.locks;

public class LockAcquireFailedException extends RuntimeException {
    private final String coordinationNodePath;
    private final String semaphoreName;

    public LockAcquireFailedException(String coordinationNodePath, String semaphoreName) {
        super("Failed to acquire semaphore=" + semaphoreName + ", on coordination node=" + coordinationNodePath);
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
