package tech.ydb.slo.core.kv;

/**
 * Result of a single KV operation as reported by a {@link KvSession}.
 *
 * <p>The workload runner turns an outcome into the SLO metrics: a success
 * records latency and the number of retry attempts; an error records the
 * {@code error_kind} label instead. Carrying the retry count here lets every
 * client (native SDK, JDBC, Spring Data) report it uniformly even though each
 * one implements retries differently.
 */
public final class OpOutcome {
    private final boolean success;
    private final int retryAttempts;
    private final String errorKind;

    private OpOutcome(boolean success, int retryAttempts, String errorKind) {
        this.success = success;
        this.retryAttempts = Math.max(0, retryAttempts);
        this.errorKind = errorKind;
    }

    /**
     * @param retryAttempts number of retries beyond the first attempt (0 if it succeeded first try)
     * @return a successful outcome
     */
    public static OpOutcome success(int retryAttempts) {
        return new OpOutcome(true, retryAttempts, null);
    }

    /**
     * @param retryAttempts number of retries beyond the first attempt
     * @param errorKind classified error label (e.g. {@code ydb/aborted})
     * @return a failed outcome
     */
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
