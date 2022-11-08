package tech.ydb.examples.bulk_upsert;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

/**
 *
 * @author Alexandr Gorshenin
 */
public class LogRecord {
    public static final StructType COLUMNS = StructType.of(
        "app", PrimitiveType.Text,
        "timestamp", PrimitiveType.Timestamp,
        "host", PrimitiveType.Text,
        "http_code", PrimitiveType.Uint32,
        "message", PrimitiveType.Text
    );

    public static final List<String> PRIMARY_KEYS = Arrays.asList(
            "app", "timestamp", "host"
    );

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

    private Map<String, Value<?>> toValue() {
        return ImmutableMap.of(
            "app", PrimitiveValue.newText(app),
            "timestamp", PrimitiveValue.newTimestamp(timestamp),
            "host", PrimitiveValue.newText(host),
            "http_code", PrimitiveValue.newUint32(httpCode),
            "message", PrimitiveValue.newText(message)
        );
    }

    public static ListValue toListValue(List<LogRecord> items) {
        ListType listType = ListType.of(COLUMNS);
        return listType.newValue(items.stream()
            .map(e -> COLUMNS.newValue(e.toValue()))
            .collect(Collectors.toList()));
    }
}
