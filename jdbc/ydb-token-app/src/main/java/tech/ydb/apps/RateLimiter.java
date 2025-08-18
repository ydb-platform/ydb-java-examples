package tech.ydb.apps;


/**
 *
 * @author Aleksandr Gorshenin
 */
public interface RateLimiter {
    void acquire();

    static RateLimiter noLimit() {
        // nothing
        return () -> { };
    }

    static RateLimiter withRps(int rps) {
        return com.google.common.util.concurrent.RateLimiter.create(rps)::acquire;
    }
}
