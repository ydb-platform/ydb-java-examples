# Topic examples for ydb.tech

This application supplies the Java snippets in the topic reference on ydb.tech.
It uses the released SDK version declared by this module and builds as part of
the repository's Maven reactor.

With JDK 8 or newer, Maven and a local YDB instance, run from the repository root:

```sh
bash examples/ydb_tech/topic/run.sh
```

`YDB_CONNECTION_STRING` defaults to `grpc://localhost:2136/local`.
The application checks management, sync and async writers/readers, acknowledgments,
metadata, compression, selectors, external commits and transactions. Topics have unique names.
The offset callback uses an in-memory store; use persistent storage for production recovery.
