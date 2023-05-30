package tech.ydb.jdbc.example;

import java.time.Instant;
import java.util.Objects;

/**
 *
 * @author Alexandr Gorshenin
 */
public class LogRecord {
    private final String app;
    private final Instant timestamp;
    private final String host;
    private final Integer httpCode;
    private final String message;

    public LogRecord(String app, Instant timestamp, String host, Integer httpCode, String message) {
        this.app = app;
        this.timestamp = timestamp;
        this.host = host;
        this.httpCode = httpCode;
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogRecord other = (LogRecord) o;

        return Objects.equals(app, other.app)
                && Objects.equals(timestamp, other.timestamp)
                && Objects.equals(host, other.host)
                && Objects.equals(httpCode, other.httpCode)
                && Objects.equals(message, other.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(app, timestamp, host, httpCode, message);
    }

    @Override
    public String toString() {
        return "LogRecord{" +
            "app=" + app +
            ", timestamp=" + timestamp +
            ", host='" + host +
            ", httpCode=" + httpCode +
            ", message=" + message +
            '}';
    }

    public String app() {
        return this.app;
    }

    public Instant timestamp() {
        return this.timestamp;
    }

    public String host() {
        return this.host;
    }

    public Integer httpCode() {
        return this.httpCode;
    }

    public String message() {
        return this.message;
    }
}
