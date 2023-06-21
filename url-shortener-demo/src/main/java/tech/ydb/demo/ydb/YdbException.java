package tech.ydb.demo.ydb;

/**
 *
 * @author Alexandr Gorshenin
 */
public class YdbException extends Exception {
    private static final long serialVersionUID = -720473130272982493L;

    public YdbException(String message, Throwable reason) {
        super(message, reason);
    }
}
