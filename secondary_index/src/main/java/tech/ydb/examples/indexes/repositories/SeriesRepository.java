package tech.ydb.examples.indexes.repositories;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Result;
import tech.ydb.examples.indexes.QueryTemplate;
import tech.ydb.examples.indexes.model.Series;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

public class SeriesRepository {
    private static final TableDescription TABLE_DESCRIPTION = TableDescription.newBuilder()
            .addNullableColumn("series_id", PrimitiveType.Uint64)
            .addNullableColumn("title", PrimitiveType.Text)
            .addNullableColumn("series_info", PrimitiveType.Text)
            .addNullableColumn("release_date", PrimitiveType.Uint32)
            .addNullableColumn("views", PrimitiveType.Uint64)
            .setPrimaryKey("series_id")
            .build();

    private static final TableDescription TABLE_REV_INDEX_DESCRIPTION = TableDescription.newBuilder()
            .addNullableColumn("rev_views", PrimitiveType.Uint64)
            .addNullableColumn("series_id", PrimitiveType.Uint64)
            .setPrimaryKeys("rev_views", "series_id")
            .build();

    private final String tablePrefix;
    private final SessionRetryContext retryCtx;

    public SeriesRepository(TableClient tableClient, String tablePrefix) {
        if (!tablePrefix.endsWith("/")) {
            tablePrefix += "/";
        }
        this.tablePrefix = tablePrefix;
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    public void dropTables() {
        retryCtx.supplyStatus(session -> session.dropTable(tablePrefix + "series")).join();
        retryCtx.supplyStatus(session -> session.dropTable(tablePrefix + "series_rev_views")).join();
    }

    public void createTables() {
        retryCtx.supplyStatus(session -> session
                .createTable(tablePrefix + "series", TABLE_DESCRIPTION))
                .join();
        retryCtx.supplyStatus(session -> session
                .createTable(tablePrefix + "series_rev_views", TABLE_REV_INDEX_DESCRIPTION))
                .join();
    }

    public void insert(Series series) {
        insertAsync(series).join();
    }

    public CompletableFuture<Boolean> insertAsync(Series series) {
        Params params = Params.of(
                "$seriesId", PrimitiveValue.newUint64(series.getSeriesId()),
                "$title", PrimitiveValue.newText(series.getTitle()),
                "$seriesInfo", PrimitiveValue.newText(series.getSeriesInfo()),
                "$releaseDate", PrimitiveValue.newUint32(series.getReleaseDate().toEpochDay()),
                "$views", PrimitiveValue.newUint64(series.getViews())
        );
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_INSERT, params))
                .thenApply(result -> result.isSuccess());
    }

    public long delete(long seriesId) {
        return deleteAsync(seriesId).join();
    }

    public CompletableFuture<Long> deleteAsync(long seriesId) {
        Params params = Params.of("$seriesId", PrimitiveValue.newUint64(seriesId));
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_DELETE, params))
                .thenApply(Result::getValue).thenApply(result -> {
                    ResultSetReader resultSet = result.getResultSet(0);
                    if (!resultSet.next()) {
                        throw new IllegalStateException("Query count was not returned");
                    }
                    return resultSet.getColumn(0).getUint64();
                });
    }

    public long updateViews(long seriesId, long newViews) {
        return updateViewsAsync(seriesId, newViews).join();
    }

    public CompletableFuture<Long> updateViewsAsync(long seriesId, long newViews) {
        Params params = Params.of(
                "$seriesId", PrimitiveValue.newUint64(seriesId),
                "$newViews", PrimitiveValue.newUint64(newViews)
        );
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_UPDATE_VIEWS, params))
                .thenApply(Result::getValue).thenApply(result -> {
                    ResultSetReader resultSet = result.getResultSet(0);
                    if (!resultSet.next()) {
                        throw new IllegalStateException("Query count was not returned");
                    }
                    return resultSet.getColumn(0).getUint64();
                });
    }

    public Series findById(long seriesId) {
        return findByIdAsync(seriesId).join();
    }

    public CompletableFuture<Series> findByIdAsync(long seriesId) {
        Params params = Params.of("$seriesId", PrimitiveValue.newUint64(seriesId));
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_FIND_BY_ID, params))
                .thenApply(Result::getValue).thenApply(result -> {
                    ResultSetReader resultSet = result.getResultSet(0);
                    if (!resultSet.next()) {
                        return null;
                    }
                    return extractSeries(resultSet);
                });
    }

    public List<Series> findAll(int limit) {
        return findAllAsync(limit).join();
    }

    public CompletableFuture<List<Series>> findAllAsync(int limit) {
        Params params = Params.of("$limit", PrimitiveValue.newUint64(limit));
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_FIND_ALL, params))
                .thenApply(Result::getValue).thenApply(result -> {
                    ResultSetReader resultSet = result.getResultSet(0);
                    List<Series> results = new ArrayList<>(resultSet.getRowCount());
                    while (resultSet.next()) {
                        results.add(extractSeries(resultSet));
                    }
                    return results;
                });
    }

    public List<Series> findAll(int limit, long lastSeriesId) {
        return findAllAsync(limit, lastSeriesId).join();
    }

    public CompletableFuture<List<Series>> findAllAsync(int limit, long lastSeriesId) {
        Params params = Params.of(
                "$limit", PrimitiveValue.newUint64(limit),
                "$lastSeriesId", PrimitiveValue.newUint64(lastSeriesId)
        );
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_FIND_ALL_NEXT, params))
                .thenApply(Result::getValue).thenApply(result -> {
                    ResultSetReader resultSet = result.getResultSet(0);
                    List<Series> results = new ArrayList<>(resultSet.getRowCount());
                    while (resultSet.next()) {
                        results.add(extractSeries(resultSet));
                    }
                    return results;
                });
    }

    public List<Series> findMostViewed(int limit) {
        return findMostViewedAsync(limit).join();
    }

    public CompletableFuture<List<Series>> findMostViewedAsync(int limit) {
        Params params = Params.of("$limit", PrimitiveValue.newUint64(limit));
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_FIND_MOST_VIEWED, params))
                .thenApply(Result::getValue).thenApply(result -> {
                    ResultSetReader resultSet = result.getResultSet(0);
                    List<Series> results = new ArrayList<>(resultSet.getRowCount());
                    while (resultSet.next()) {
                        results.add(extractSeries(resultSet));
                    }
                    return results;
                });
    }

    public List<Series> findMostViewed(int limit, long lastSeriesId, long lastViews) {
        return findMostViewedAsync(limit, lastSeriesId, lastViews).join();
    }

    public CompletableFuture<List<Series>> findMostViewedAsync(int limit, long lastSeriesId, long lastViews) {
        Params params = Params.of(
                "$limit", PrimitiveValue.newUint64(limit),
                "$lastSeriesId", PrimitiveValue.newUint64(lastSeriesId),
                "$lastViews", PrimitiveValue.newUint64(lastViews)
        );
        
        return retryCtx
                .supplyResult(session -> executeQuery(session, QueryTemplate.SERIES_FIND_MOST_VIEWED_NEXT, params))
                .thenApply(Result::getValue).thenApply(result -> {
                    ResultSetReader resultSet = result.getResultSet(0);
                    List<Series> results = new ArrayList<>(resultSet.getRowCount());
                    while (resultSet.next()) {
                        results.add(extractSeries(resultSet));
                    }
                    return results;
                });
    }

    private static Series extractSeries(ResultSetReader resultSet) {
        long seriesId = resultSet.getColumn(0).getUint64();
        String title = resultSet.getColumn(1).getText();
        String seriesInfo = resultSet.getColumn(2).getText();
        LocalDate releaseDate = LocalDate.ofEpochDay(resultSet.getColumn(3).getUint32());
        long views = resultSet.getColumn(4).getUint64();
        return new Series(seriesId, title, seriesInfo, releaseDate, views);
    }
    
    private CompletableFuture<Result<DataQueryResult>> executeQuery(Session session, QueryTemplate query, Params params) {
        String queryText = query.getText().replaceAll("<TABLE_PREFIX>", tablePrefix);
        TxControl<?> tx = TxControl.serializableRw().setCommitTx(true);
        return session.executeDataQuery(queryText, tx, params);
    }
}
