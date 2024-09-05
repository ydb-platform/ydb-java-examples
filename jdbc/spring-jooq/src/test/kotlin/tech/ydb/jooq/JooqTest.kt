package tech.ydb.jooq

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tech.ydb.jooq.repository.EpisodesRepository
import tech.ydb.jooq.repository.SeasonsRepository
import tech.ydb.jooq.repository.SeriesRepository
import tech.ydb.test.junit5.YdbHelperExtension
import ydb.default_schema.tables.records.EpisodesRecord
import java.time.LocalDate

/**
 * @author Kirill Kurdyukov
 */
@SpringBootTest
class JooqTest {

    companion object {
        @JvmField
        @RegisterExtension
        val ydb = YdbHelperExtension()

        @JvmStatic
        @DynamicPropertySource
        fun propertySource(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") {
                "jdbc:ydb:${if (ydb.useTls()) "grpcs://" else "grpc://"}" +
                        "${ydb.endpoint()}${ydb.database()}${ydb.authToken()?.let { "?token=$it" } ?: ""}"
            }
        }
    }

    @Autowired
    lateinit var episodesRepository: EpisodesRepository

    @Autowired
    lateinit var seasonsRepository: SeasonsRepository

    @Autowired
    lateinit var ydbDSLContext: YdbDSLContext

    @Autowired
    lateinit var seriesRepository: SeriesRepository

    @Test
    fun findAllTest() {
        assertEquals(70, episodesRepository.findAll().size)
    }

    @Test
    fun selectOrderByPkAndConditionalTest() {
        val episodes = episodesRepository.selectOrderByPkAndConditional(1, 1)

        assertEquals("The Work Outing", episodes[0].title)
        assertEquals("Return of the Golden Child", episodes[1].title)
        assertEquals("Moss and the German", episodes[2].title)
    }

    @Test
    fun aggregateCountByEpisodesTest() {
        val episodes = episodesRepository.aggregateCountByEpisodes(1)

        assertEquals(4, episodes.size)
        assertEquals(Triple(1, 1, 6).toString(), episodes[0].toString())
        assertEquals(Triple(1, 2, 6).toString(), episodes[1].toString())
        assertEquals(Triple(1, 3, 6).toString(), episodes[2].toString())
        assertEquals(Triple(1, 4, 6).toString(), episodes[3].toString())
    }

    @Test
    fun `replace upsert insert test`() {
        val episodesExpected = EpisodesRecord().apply {
            seriesId = 2
            seasonId = 5
            episodeId = 10
            title = "Test Episode !!!"
            airDate = LocalDate.parse("2018-08-27")
        }

        val check = {
            val episodeActual = episodesRepository.findByPk(2, 5, 10)
            assertNotNull(episodeActual)

            assertEquals(episodesExpected, episodeActual)

            episodesRepository.delete(2, 5, 10)
            assertNull(episodesRepository.findByPk(2, 5, 10))
        }

        episodesRepository.replace(episodesExpected)
        check()

        episodesRepository.upsert(episodesExpected)
        check()

        episodesRepository.insert(episodesExpected)
        val episodeActual = episodesRepository.findByPk(2, 5, 10)
        assertNotNull(episodeActual)
        assertEquals(episodesExpected, episodeActual)

        episodesRepository.update(episodesExpected.apply { title = "test Episode 2" })
        check()
    }

    @Test
    fun transactionTest() {
        val episodesExpected = EpisodesRecord().apply {
            seriesId = 2
            seasonId = 5
            episodeId = 10
            title = "Test Episode !!!"
            airDate = LocalDate.parse("2018-08-27")
        }

        ydbDSLContext.transaction { tx ->
            val dsl = YDB.using(tx)

            episodesRepository.upsert(episodesExpected, dsl)
            episodesRepository.update(episodesExpected.apply { title = "test Episode 2" }, dsl)
        }

        val episodeActual = episodesRepository.findByPk(2, 5, 10)
        assertEquals(episodesExpected, episodeActual)
        episodesRepository.delete(2, 5, 10)
        assertNull(episodesRepository.findByPk(2, 5, 10))
    }

    @Test
    fun joinText() {
        val titles = seasonsRepository.seasonsBySeriesId(1)

        assertEquals(4, titles.size)
        for (i in 1 ..  4) {
            assertEquals("Season $i", titles[i - 1].first)
            assertEquals("IT Crowd", titles[i - 1].second)
        }
    }
    
    @Test
    fun findByTitleViewIndex() {
        val record = seriesRepository.findByTitle("IT Crowd")!!
        assertEquals(LocalDate.parse("2006-02-03"), record.releaseDate)
    }
}