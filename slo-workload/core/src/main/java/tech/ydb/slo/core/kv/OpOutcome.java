package tech.ydb.slo.core.kv;

public final class OpOutcome {
    private final boolean success;
    private final int retryAttempts;
    private final String errorKind;
    private final Throwable cause;

    private OpOutcome(boolean success, int retryAttempts, String errorKind, Throwable cause) {
        this.success = success;
        this.retryAttempts = Math.max(0, retryAttempts);
        this.errorKind = errorKind;
        this.cause = cause;
    }

    public static OpOutcome success(int retryAttempts) {
        return new OpOutcome(true, retryAttempts, null, null);
    }

    public static OpOutcome error(int retryAttempts, String errorKind) {
        return error(retryAttempts, errorKind, null);
    }

    public static OpOutcome error(int retryAttempts, String errorKind, Throwable cause) {
        return new OpOutcome(false, retryAttempts, errorKind, cause);
    }

    public Throwable cause() {
        return cause;
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
