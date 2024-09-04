package tech.ydb.jooq.repository

import org.jooq.impl.DSL
import org.springframework.stereotype.Repository
import tech.ydb.jooq.YdbDSLContext
import ydb.default_schema.Tables.EPISODES
import ydb.default_schema.tables.records.EpisodesRecord

/**
 * @author Kirill Kurdyukov
 */
@Repository
class EpisodesRepository(val ydbDSLContext: YdbDSLContext) {

    fun findAll(): Array<EpisodesRecord> = ydbDSLContext.selectFrom(EPISODES).fetchArray()

    fun findByPk(
        seriesId: Long,
        seasonId: Long,
        episodesId: Long,
        context: YdbDSLContext = ydbDSLContext,
    ): EpisodesRecord? = context
        .selectFrom(EPISODES)
        .where(
            EPISODES.SERIES_ID.eq(seriesId)
                .and(EPISODES.SEASON_ID.eq(seasonId))
                .and(EPISODES.EPISODE_ID.eq(episodesId))
        )
        .fetchOne()

    fun selectOrderByPkAndConditional(
        seriesId: Long,
        seasonId: Long,
        limit: Int = 3,
        context: YdbDSLContext = ydbDSLContext,
    ): Array<EpisodesRecord> = context
        .selectFrom(EPISODES)
        .where(EPISODES.SERIES_ID.eq(seriesId).and(EPISODES.SEASON_ID.greaterThan(seasonId)))
        .orderBy(EPISODES.SERIES_ID, EPISODES.SEASON_ID, EPISODES.EPISODE_ID)
        .limit(limit)
        .fetchArray()

    fun aggregateCountByEpisodes(
        seriesId: Long,
        context: YdbDSLContext = ydbDSLContext,
    ): List<Triple<Long, Long, Int>> = context
        .select(EPISODES.SERIES_ID, EPISODES.SEASON_ID, DSL.count().`as`("cnt"))
        .from(EPISODES)
        .where(EPISODES.SERIES_ID.eq(seriesId))
        .groupBy(EPISODES.SERIES_ID, EPISODES.SEASON_ID)
        .orderBy(EPISODES.SERIES_ID, EPISODES.SEASON_ID)
        .fetch { rec -> Triple(rec.value1(), rec.value2(), rec.value3()) }

    fun replace(record: EpisodesRecord, context: YdbDSLContext = ydbDSLContext) {
        context.replaceInto(EPISODES)
            .set(record)
            .execute()
    }

    fun upsert(record: EpisodesRecord, context: YdbDSLContext = ydbDSLContext) {
        context.upsertInto(EPISODES)
            .set(record)
            .execute()
    }

    fun insert(record: EpisodesRecord, context: YdbDSLContext = ydbDSLContext) {
        context.insertInto(EPISODES)
            .set(record)
            .execute()
    }

    fun update(record: EpisodesRecord, context: YdbDSLContext = ydbDSLContext) {
        context.update(EPISODES)
            .set(EPISODES.TITLE.`as`(EPISODES.TITLE), record.title)
            .where(
                EPISODES.SERIES_ID.eq(record.seriesId).and(EPISODES.SEASON_ID.eq(record.seasonId))
                    .and(EPISODES.EPISODE_ID.eq(record.episodeId))
            )
            .execute()
    }

    fun delete(seriesId: Long, seasonId: Long, episodesId: Long, context: YdbDSLContext = ydbDSLContext) {
        context.delete(EPISODES)
            .where(
                EPISODES.SERIES_ID.eq(seriesId).and(EPISODES.SEASON_ID.eq(seasonId))
                    .and(EPISODES.EPISODE_ID.eq(episodesId))
            ).execute()
    }
}