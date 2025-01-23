package tech.ydb.app;

import tech.ydb.app.repository.YdbRepository;

public class App {
    public static void main(String[] args) {
        if (args.length != 1) {
            throw new UnsupportedOperationException("Expected 1 parameter connectionString");
        }

        new YdbRepository(args[0]).TestDatabase();
    }
}
