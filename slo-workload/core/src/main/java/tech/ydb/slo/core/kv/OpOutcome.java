package tech.ydb.slo.core.kv;

public final class OpOutcome {
    private final boolean success;
    private final int retryAttempts;
    private final String errorKind;

    private OpOutcome(boolean success, int retryAttempts, String errorKind) {
        this.success = success;
        this.retryAttempts = Math.max(0, retryAttempts);
        this.errorKind = errorKind;
    }



    public static OpOutcome success(int retryAttempts) {
        return new OpOutcome(true, retryAttempts, null);
    }



    public static OpOutcome error(int retryAttempts, String errorKind) {
        return new OpOutcome(false, retryAttempts, errorKind);
    }

    public boolean isSuccess() {
        return success;
    }

    public int retryAttempts() {
        return retryAttempts;
    }

    public String errorKind() {
        return errorKind;
    }
}
