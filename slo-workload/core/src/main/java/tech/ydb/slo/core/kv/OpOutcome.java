package tech.ydb.slo.core.kv;

public final class OpOutcome {
    private final boolean success;
    private final int retryAttempts;
    private final String errorKind;
    private final Throwable cause;
    private final java.util.List<Row> rows;

    private OpOutcome(boolean success, int retryAttempts, String errorKind, Throwable cause,
            java.util.List<Row> rows) {
        this.success = success;
        this.retryAttempts = Math.max(0, retryAttempts);
        this.errorKind = errorKind;
        this.cause = cause;
        this.rows = java.util.List.copyOf(rows);
    }

    public static OpOutcome success(int retryAttempts) {
        return success(retryAttempts, java.util.List.of());
    }

    public static OpOutcome success(int retryAttempts, java.util.List<Row> rows) {
        return new OpOutcome(true, retryAttempts, null, null, rows);
    }

    public java.util.List<Row> rows() {
        return rows;
    }

    public static OpOutcome error(int retryAttempts, String errorKind) {
        return error(retryAttempts, errorKind, null);
    }

    public static OpOutcome error(int retryAttempts, String errorKind, Throwable cause) {
        return new OpOutcome(false, retryAttempts, errorKind, cause, java.util.List.of());
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
