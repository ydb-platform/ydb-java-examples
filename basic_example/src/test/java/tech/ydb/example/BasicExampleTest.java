package tech.ydb.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BasicExampleTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static String connectionString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ydb.useTls() ? "grpcs://" : "grpc://" );
        sb.append(ydb.endpoint());
        sb.append(ydb.database());
        return sb.toString();
    }

    @Test
    public void testBasicApp() {
        App app = new App(connectionString());
        app.run();
        app.close();
    }
}
