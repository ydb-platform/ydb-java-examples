package tech.ydb.jdbc.failsafe;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;


public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    private final static RetryPolicy<?> DEFAULT_POLICY = RetryPolicy.builder()
            .handle(SQLRecoverableException.class)
            .withBackoff(100, 1000, ChronoUnit.MILLIS, 2)
            .withMaxDuration(Duration.ofSeconds(5))
            .build();

    private final static RetryPolicy<?> IDEMPOTENT_POLICY = RetryPolicy.builder()
            .handle(SQLRecoverableException.class, SQLTransientException.class)
            .withBackoff(100, 1000, ChronoUnit.MILLIS, 2)
            .withMaxDuration(Duration.ofSeconds(5))
            .build();

    public static void main(String[] args) {
        // Enable redirect Java Util Logging to SLF4J
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("").setLevel(Level.FINEST);

        if (args.length != 1) {
            System.err.println("Usage: java -jar ydb-failsafe-example.jar <connection-string>");
            return;
        }

        String jdbcURL = args[0];
        logger.info("connect to {}", jdbcURL);

        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            connection.setAutoCommit(true);

            createTables(connection);
            upsertTablesData(connection);

            upsertSimple(connection);

            selectSimple(connection);
            selectWithParams(connection, 1, 2);
            scanQueryWithParams(connection, 2, 1);

            connection.setAutoCommit(false);
            tclTransaction(connection);

            connection.setAutoCommit(true);
            dropTables(connection);
        } catch (SQLException e) {
            logger.error("JDBC Connection problem", e);
        }
    }

    private static void createTables(Connection connection) {
        Failsafe.with(DEFAULT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(""
                        + "CREATE TABLE series ("
                        + "  series_id Int64,"
                        + "  title Text,"
                        + "  series_info Text,"
                        + "  release_date Date,"
                        + "  PRIMARY KEY(series_id)"
                        + ")"
                );
            }
        });

        Failsafe.with(DEFAULT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(""
                        + "CREATE TABLE seasons ("
                        + "  series_id Int64,"
                        + "  season_id Int64,"
                        + "  title Text,"
                        + "  first_aired Date,"
                        + "  last_aired Date,"
                        + "  PRIMARY KEY(series_id, season_id)"
                        + ")"
                );
            }
        });

        Failsafe.with(DEFAULT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(""
                        + "CREATE TABLE episodes ("
                        + "  series_id Int64,"
                        + "  season_id Int64,"
                        + "  episode_id Int64,"
                        + "  title Text,"
                        + "  air_date Date,"
                        + "  PRIMARY KEY(series_id, season_id, episode_id)"
                        + ")"
                );
            }
        });
    }

    private static void upsertTablesData(Connection connection) {
        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPSERT INTO series (series_id, title, series_info, release_date) VALUES (?, ?, ?, ?)"
            )) {
                for (AppData.Series series: AppData.SERIES) {
                    statement.setLong(1, series.seriesID());
                    statement.setString(2, series.title());
                    statement.setString(3, series.seriesInfo());
                    statement.setDate(4, series.releaseDate());
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        });

        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPSERT INTO seasons (series_id, season_id, title, first_aired, last_aired) VALUES (?, ?, ?, ?, ?)"
            )) {
                for (AppData.Season season: AppData.SEASONS) {
                    statement.setLong(1, season.seriesID());
                    statement.setLong(2, season.seasonID());
                    statement.setString(3, season.title());
                    statement.setDate(4, season.firstAired());
                    statement.setDate(5, season.lastAired());
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        });

        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date) VALUES (?, ?, ?, ?, ?)"
            )) {
                for (AppData.Episode episode: AppData.EPISODES) {
                    statement.setLong(1, episode.seriesID());
                    statement.setLong(2, episode.seasonID());
                    statement.setLong(3, episode.episodeID());
                    statement.setString(4, episode.title());
                    statement.setDate(5, episode.airDate());
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        });
    }

    private static void upsertSimple(Connection connection) {
        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES (2, 6, 1, \"TBD\");"
                );
            }
        });
    }

    private static void selectSimple(Connection connection) {
        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(
                        "SELECT series_id, title, release_date FROM series WHERE series_id = 1;"
                );

                logger.info("--[ SelectSimple ]--");
                while (rs.next()) {
                    logger.info("read series with id {}, title {} and release_date {}",
                            rs.getLong("series_id"),
                            rs.getString("title"),
                            rs.getDate("release_date")
                    );
                }
            }
        });
    }

    private static void selectWithParams(Connection connection, long seriesID, long seasonID) {
        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (PreparedStatement statement = connection.prepareStatement(""
                    + "SELECT sa.title AS season_title, sr.title AS series_title "
                    + "FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id "
                    + "WHERE sa.series_id = ? AND sa.season_id = ?"
            )) {

                statement.setLong(1, seriesID);
                statement.setLong(2, seasonID);

                ResultSet rs = statement.executeQuery();

                logger.info("--[ SelectWithParams ]--");
                while (rs.next()) {
                    logger.info("read season with title {} for series {}",
                            rs.getString("season_title"),
                            rs.getString("series_title")
                    );
                }
            }
        });
    }

    private static void scanQueryWithParams(Connection connection, long seriesID, long seasonID) {
        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (PreparedStatement statement = connection.prepareStatement(""
                    + "SCAN SELECT ep.title AS episode_title, sa.title AS season_title, sr.title AS series_title "
                    + "FROM episodes AS ep "
                    + "JOIN seasons AS sa ON sa.season_id = ep.season_id "
                    + "JOIN series AS sr ON sr.series_id = sa.series_id "
                    + "WHERE sa.series_id =? AND sa.season_id = ?;"
            )) {

                statement.setLong(1, seriesID);
                statement.setLong(2, seasonID);

                ResultSet rs = statement.executeQuery();

                logger.info("--[ ExecuteScanQueryWithParams ]--");
                while (rs.next()) {
                    logger.info("read episode {} of {} for {}",
                            rs.getString("episode_title"),
                            rs.getString("season_title"),
                            rs.getString("series_title")
                    );
                }
            }
        });
    }

    private static void tclTransaction(Connection connection) {
        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            connection.setAutoCommit(false);

            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE episodes SET air_date = ? WHERE title = \"TBD\";"
            )) {
                ps.setDate(1, Date.valueOf(LocalDate.now()));
                ps.execute();
            }

            connection.commit();
        });
    }

    private static void dropTables(Connection connection) {
        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE episodes");
            }
        });

        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE seasons");
            }
        });

        Failsafe.with(IDEMPOTENT_POLICY).run(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE series");
            }
        });
    }
}
