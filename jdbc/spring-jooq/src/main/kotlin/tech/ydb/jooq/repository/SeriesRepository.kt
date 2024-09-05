package tech.ydb.jooq.repository

import org.springframework.stereotype.Repository
import tech.ydb.jooq.YdbDSLContext
import ydb.default_schema.Indexes
import ydb.default_schema.Tables.SERIES

/**
 * @author Kirill Kurdyukov
 */
@Repository
class SeriesRepository(val ydbDSLContext: YdbDSLContext) {

    fun findByTitle(title: String) = ydbDSLContext.selectFrom(SERIES.useIndex(Indexes.TITLE_NAME.name))
        .where(SERIES.TITLE.eq(title))
        .fetchOne()
}