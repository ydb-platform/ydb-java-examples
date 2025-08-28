## YDB Token Application
## A simple example of Spring Boot 2 Application working with YDB Database

### How to build

Requirements
* Java 17 or newer
* Maven 3.0.0 or newer

To build the application as a single executable jar file, run the command:
```
cd ydb-java-examples/jdbc/ydb-token-app
mvn clean package spring-boot:repackage
```
After that, the compiled `ydb-token-app-1.1.0-SNAPSHOT.jar` can be found in the target folder.

### What this application does

This application allows you to create a test table called `app_token` in the YDB database, populate it with data, and
launch a test workload for parallel reading and writing to this table. During the test, the following operations will be
performed in parallel in several threads:
* Read a random token from the database - 50% of operations
* Read and update a random token in the database - 40% of operations
* Read and update 100 random tokens in the database - 10% of operations

The statistics collected during the test include the number of operations performed, RPS (requests per second), and
average execution time for each type of operation. There is also support for exporting application metrics in Prometheus
format.

### How to launch

The application is built as a single executable jar file and can be run with the command:
```
java -jar ydb-token-app-1.1.0-SNAPSHOT.jar <options> <commands>
```
Where `options` are application parameters (see the Application Parameters section), and `commands` are the sequence of
commands the application will execute one after the other. Currently, the following commands are supported:
* clean    - clean the database, the `app_token` table will be deleted
* init     - prepare the database, the empty `app_token` table will be created
* load     - load test data, the `app_token` table will be filled with initial data
* run      - start the test workload
* validate - validate current data stored in database

Commands can be used individually or sequenced, for example:

Recreate the `app_token` table and initialize it with initial data:
```
java -jar ydb-token-app-1.1.0-SNAPSHOT.jar --app.connection=grpcs://my-ydb:2135/my-database clean init load
```

Start the test and then clean the database:
```
java -jar ydb-token-app-1.1.0-SNAPSHOT.jar --app.connection=grpcs://my-ydb:2135/my-database run clean
```

Recreate the `app_token` table, initialize it with data, and start the test:
```
java -jar ydb-token-app-1.1.0-SNAPSHOT.jar --app.connection=grpcs://my-ydb:2135/my-database clean init load run
```

### Application parameters

Application parameters allow you to configure different aspects of the application's operation, primarily the database connection address.
The main parameters list:

* `app.connection` - database connection address. Specified as `<schema>://<endpoint>:<port>/<database>`
* `app.threadsCount` - number of threads the application creates. Defaults to the number of CPU cores on the host.
* `app.recordsCount` - number of records in the table used for testing. Default is 1 million.
* `app.load.batchSize` - batch size for loading data when running the load command. Default is 1000.
* `app.workload.duration` - test duration in seconds when running the run command. Default is 60 seconds.
* `app.rpsLimit` - limit on the number of operations per second during the run command. By default, there is no limit (-1).
* `app.pushMetrics` - flag indicating whether metrics should be exported to Prometheus; disabled by default.
* `app.prometheusUrl` - endpoint of Prometheus to export metrics to. Default is http://localhost:9091.

All parameters can be passed directly when launching the application (in the format `--param_name=value`) or can be
preconfigured in an `application.properties` file saved next to the executable jar of the application.
