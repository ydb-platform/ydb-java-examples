package tech.ydb.jooq.repository

import org.springframework.stereotype.Repository
import tech.ydb.jooq.YdbDSLContext
import ydb.default_schema.Tables.SEASONS
import ydb.default_schema.Tables.SERIES

/**
 * @author Kirill Kurdyukov
 */
@Repository
class SeasonsRepository(val ydbDSLContext: YdbDSLContext) {

    fun seasonsBySeriesId(seriesId: Long, context: YdbDSLContext = ydbDSLContext): List<Pair<String, String>> = context
        .select(
            SEASONS.TITLE.`as`("seasons_title"),
            SERIES.TITLE.`as`("series_title"),
            SERIES.SERIES_ID,
            SEASONS.SEASON_ID
        )
        .from(SEASONS)
        .innerJoin(SERIES)
        .on(SERIES.SERIES_ID.eq(SEASONS.SERIES_ID))
        .where(SERIES.SERIES_ID.eq(seriesId))
        .orderBy(SERIES.SERIES_ID, SEASONS.SEASON_ID)
        .fetch { rec -> rec.value1() to rec.value2() }
}