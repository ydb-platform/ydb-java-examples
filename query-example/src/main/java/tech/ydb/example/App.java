package tech.ydb.example;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;


public final class App implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    private final GrpcTransport transport;
    private final QueryClient queryClient;
    private final SessionRetryContext retryCtx;

    App(String connectionString) {
        this.transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build();
        this.queryClient = QueryClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(queryClient).build();
    }

    @Override
    public void run() {
        createTables();
        upsertTablesData();

        upsertSimple();

        selectSimple();
        selectWithParams(1, 2);
        asyncSelectRead(2, 1);

        multiStepTransaction(2, 5);
        tclTransaction();

        dropTables();
    }

    @Override
    public void close() {
        queryClient.close();
        transport.close();
    }

    private void createTables() {
        retryCtx.supplyResult(session -> session.createQuery(""
                + "CREATE TABLE series ("
                + "  series_id UInt64,"
                + "  title Text,"
                + "  series_info Text,"
                + "  release_date Date,"
                + "  PRIMARY KEY(series_id)"
                + ")", TxMode.NONE).execute()
        ).join().getStatus().expectSuccess("Can't create table series");

        retryCtx.supplyResult(session -> session.createQuery(""
                + "CREATE TABLE seasons ("
                + "  series_id UInt64,"
                + "  season_id UInt64,"
                + "  title Text,"
                + "  first_aired Date,"
                + "  last_aired Date,"
                + "  PRIMARY KEY(series_id, season_id)"
                + ")", TxMode.NONE).execute()
        ).join().getStatus().expectSuccess("Can't create table seasons");

        retryCtx.supplyResult(session -> session.createQuery(""
                + "CREATE TABLE episodes ("
                + "  series_id UInt64,"
                + "  season_id UInt64,"
                + "  episode_id UInt64,"
                + "  title Text,"
                + "  air_date Date,"
                + "  PRIMARY KEY(series_id, season_id, episode_id)"
                + ")", TxMode.NONE).execute()
        ).join().getStatus().expectSuccess("Can't create table episodes");
    }

    private void upsertTablesData() {
        // Create type for struct of series
        StructType seriesType = StructType.of(
                "series_id", PrimitiveType.Uint64,
                "title", PrimitiveType.Text,
                "release_date", PrimitiveType.Date,
                "series_info", PrimitiveType.Text
        );
        // Create and fill list of series
        ListValue seriesData = ListType.of(seriesType).newValue(
                SeriesData.SERIES.stream().map(series -> seriesType.newValue(
                        "series_id", PrimitiveValue.newUint64(series.seriesID()),
                        "title", PrimitiveValue.newText(series.title()),
                        "release_date", PrimitiveValue.newDate(series.releaseDate()),
                        "series_info", PrimitiveValue.newText(series.seriesInfo())
                )).collect(Collectors.toList())
        );

        // Upsert list of series to table
        retryCtx.supplyResult(session -> session.createQuery(
                "UPSERT INTO series SELECT * FROM AS_TABLE($values)",
                TxMode.SERIALIZABLE_RW,
                Params.of("$values", seriesData)
        ).execute()).join().getStatus().expectSuccess("upsert problem");


        // Create type for struct of season
        StructType seasonType = StructType.of(
                "series_id", PrimitiveType.Uint64,
                "season_id", PrimitiveType.Uint64,
                "title", PrimitiveType.Text,
                "first_aired", PrimitiveType.Date,
                "last_aired", PrimitiveType.Date
        );
        // Create and fill list of seasons
        ListValue seasonsData = ListType.of(seasonType).newValue(
                SeriesData.SEASONS.stream().map(season -> seasonType.newValue(
                        "series_id", PrimitiveValue.newUint64(season.seriesID()),
                        "season_id", PrimitiveValue.newUint64(season.seasonID()),
                        "title", PrimitiveValue.newText(season.title()),
                        "first_aired", PrimitiveValue.newDate(season.firstAired()),
                        "last_aired", PrimitiveValue.newDate(season.lastAired())
                )).collect(Collectors.toList())
        );

        // Upsert list of seasons to table
        retryCtx.supplyResult(session -> session.createQuery(
                "UPSERT INTO seasons SELECT * FROM AS_TABLE($values)",
                TxMode.SERIALIZABLE_RW,
                Params.of("$values", seasonsData)
        ).execute()).join().getStatus().expectSuccess("upsert problem");


        // Create type for struct of episode
        StructType episodeType = StructType.of(
                "series_id", PrimitiveType.Uint64,
                "season_id", PrimitiveType.Uint64,
                "episode_id", PrimitiveType.Uint64,
                "title", PrimitiveType.Text,
                "air_date", PrimitiveType.Date
        );
        // Create and fill list of episodes
        ListValue episodesData = ListType.of(episodeType).newValue(
                SeriesData.EPISODES.stream().map(episode -> episodeType.newValue(
                        "series_id", PrimitiveValue.newUint64(episode.seriesID()),
                        "season_id", PrimitiveValue.newUint64(episode.seasonID()),
                        "episode_id", PrimitiveValue.newUint64(episode.episodeID()),
                        "title", PrimitiveValue.newText(episode.title()),
                        "air_date", PrimitiveValue.newDate(episode.airDate())
                )).collect(Collectors.toList())
        );

        // Upsert list of series to episodes
        retryCtx.supplyResult(session -> session.createQuery(
                "UPSERT INTO episodes SELECT * FROM AS_TABLE($values)",
                TxMode.SERIALIZABLE_RW,
                Params.of("$values", episodesData)
        ).execute()).join().getStatus().expectSuccess("upsert problem");
    }

    private void upsertSimple() {
        String query
                = "UPSERT INTO episodes (series_id, season_id, episode_id, title) "
                + "VALUES (2, 6, 1, \"TBD\");";

        // Executes data query with specified transaction control settings.
        retryCtx.supplyResult(session -> session.createQuery(query, TxMode.SERIALIZABLE_RW).execute())
            .join().getValue();
    }

    private void selectSimple() {
        String query
                = "SELECT series_id, title, release_date "
                + "FROM series WHERE series_id = 1;";

        // Executes data query with specified transaction control settings.
        QueryReader result = retryCtx.supplyResult(
                session -> QueryReader.readFrom(session.createQuery(query, TxMode.SERIALIZABLE_RW))
        ).join().getValue();

        logger.info("--[ SelectSimple ]--");

        ResultSetReader rs = result.getResultSet(0);
        while (rs.next()) {
            logger.info("read series with id {}, title {} and release_date {}",
                    rs.getColumn("series_id").getUint64(),
                    rs.getColumn("title").getText(),
                    rs.getColumn("release_date").getDate()
            );
        }
    }

    private void selectWithParams(long seriesID, long seasonID) {
        String query
                = "DECLARE $seriesId AS Uint64; "
                + "DECLARE $seasonId AS Uint64; "
                + "SELECT sa.title AS season_title, sr.title AS series_title "
                + "FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id "
                + "WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId";

        // Type of parameter values should be exactly the same as in DECLARE statements.
        Params params = Params.of(
                "$seriesId", PrimitiveValue.newUint64(seriesID),
                "$seasonId", PrimitiveValue.newUint64(seasonID)
        );

        QueryReader result = retryCtx.supplyResult(
                session -> QueryReader.readFrom(session.createQuery(query, TxMode.SNAPSHOT_RO, params))
        ).join().getValue();

        logger.info("--[ SelectWithParams ] -- ");

        ResultSetReader rs = result.getResultSet(0);
        while (rs.next()) {
            logger.info("read season with title {} for series {}",
                    rs.getColumn("season_title").getText(),
                    rs.getColumn("series_title").getText()
            );
        }
    }

    private void asyncSelectRead(long seriesID, long seasonID) {
        String query
                = "DECLARE $seriesId AS Uint64; "
                + "DECLARE $seasonId AS Uint64; "
                + "SELECT ep.title AS episode_title, sa.title AS season_title, sr.title AS series_title "
                + "FROM episodes AS ep "
                + "JOIN seasons AS sa ON sa.season_id = ep.season_id "
                + "JOIN series AS sr ON sr.series_id = sa.series_id "
                + "WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;";

        // Type of parameter values should be exactly the same as in DECLARE statements.
        Params params = Params.of(
                "$seriesId", PrimitiveValue.newUint64(seriesID),
                "$seasonId", PrimitiveValue.newUint64(seasonID)
        );

        logger.info("--[ ExecuteAsyncQueryWithParams ]--");
        retryCtx.supplyResult(session -> {
            QueryStream asyncQuery = session.createQuery(query, TxMode.SNAPSHOT_RO, params);
            return asyncQuery.execute(part -> {
                ResultSetReader rs = part.getResultSetReader();
                logger.info("read {} rows of result set {}", rs.getRowCount(), part.getResultSetIndex());
                while (rs.next()) {
                    logger.info("read episode {} of {} for {}",
                            rs.getColumn("episode_title").getText(),
                            rs.getColumn("season_title").getText(),
                            rs.getColumn("series_title").getText()
                    );
                }
            });
        }).join().getStatus().expectSuccess("execute query problem");
    }

    private void multiStepTransaction(long seriesID, long seasonID) {
        retryCtx.supplyStatus(session -> {
            QueryTransaction transaction = session.createNewTransaction(TxMode.SNAPSHOT_RO);
            String query1
                    = "DECLARE $seriesId AS Uint64; "
                    + "DECLARE $seasonId AS Uint64; "
                    + "SELECT MIN(first_aired) AS from_date FROM seasons "
                    + "WHERE series_id = $seriesId AND season_id = $seasonId;";

            // Execute first query to start a new transaction
            QueryReader res1 = QueryReader.readFrom(transaction.createQuery(query1, Params.of(
                    "$seriesId", PrimitiveValue.newUint64(seriesID),
                    "$seasonId", PrimitiveValue.newUint64(seasonID)
            ))).join().getValue();

            // Perform some client logic on returned values
            ResultSetReader resultSet = res1.getResultSet(0);
            if (!resultSet.next()) {
                throw new RuntimeException("not found first_aired");
            }
            LocalDate fromDate = resultSet.getColumn("from_date").getDate();
            LocalDate toDate = fromDate.plusDays(15);

            // Get active transaction id
            logger.info("started new transaction {}", transaction.getId());

            // Construct next query based on the results of client logic
            String query2
                    = "DECLARE $seriesId AS Uint64;"
                    + "DECLARE $fromDate AS Date;"
                    + "DECLARE $toDate AS Date;"
                    + "SELECT season_id, episode_id, title, air_date FROM episodes "
                    + "WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate;";

            // Execute second query with commit at end.
            QueryReader res2 = QueryReader.readFrom(transaction.createQueryWithCommit(query2, Params.of(
                "$seriesId", PrimitiveValue.newUint64(seriesID),
                "$fromDate", PrimitiveValue.newDate(fromDate),
                "$toDate", PrimitiveValue.newDate(toDate)
            ))).join().getValue();

            logger.info("--[ MultiStep ]--");
            ResultSetReader rs = res2.getResultSet(0);
            while (rs.next()) {
                logger.info("read episode {} with air date {}",
                        rs.getColumn("title").getText(),
                        rs.getColumn("air_date").getDate()
                );
            }

            return CompletableFuture.completedFuture(Status.SUCCESS);
        }).join().expectSuccess("multistep transaction problem");
    }

    private void tclTransaction() {
        retryCtx.supplyResult(session -> {
            QueryTransaction transaction = session.beginTransaction(TxMode.SERIALIZABLE_RW)
                .join().getValue();

            String query
                    = "DECLARE $airDate AS Date; "
                    + "UPDATE episodes SET air_date = $airDate WHERE title = \"TBD\";";

            Params params = Params.of("$airDate", PrimitiveValue.newDate(Instant.now()));

            // Execute data query.
            // Transaction control settings continues active transaction (tx)
            QueryReader reader = QueryReader.readFrom(transaction.createQuery(query, params))
                .join().getValue();

            logger.info("get transaction {}", transaction.getId());

            // Commit active transaction (tx)
            return transaction.commit();
        }).join().getStatus().expectSuccess("tcl transaction problem");
    }

    private void dropTables() {
        retryCtx.supplyResult(session -> session.createQuery("DROP TABLE episodes;", TxMode.NONE).execute())
                .join().getStatus().expectSuccess("drop table episodes problem");
        retryCtx.supplyResult(session -> session.createQuery("DROP TABLE seasons;", TxMode.NONE).execute())
                .join().getStatus().expectSuccess("drop table seasons problem");
        retryCtx.supplyResult(session -> session.createQuery("DROP TABLE series;", TxMode.NONE).execute())
                .join().getStatus().expectSuccess("drop table series problem");
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar ydb-basic-example.jar <connection-string>");
            return;
        }

        try (App app = new App(args[0])) {
            app.run();
        } catch (Exception e) {
            logger.error("app problem", e);
        }
    }
}
