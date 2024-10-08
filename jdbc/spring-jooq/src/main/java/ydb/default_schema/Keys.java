/*
 * This file is generated by jOOQ.
 */
package ydb.default_schema;


import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;

import ydb.default_schema.tables.Episodes;
import ydb.default_schema.tables.Seasons;
import ydb.default_schema.tables.Series;
import ydb.default_schema.tables.records.EpisodesRecord;
import ydb.default_schema.tables.records.SeasonsRecord;
import ydb.default_schema.tables.records.SeriesRecord;


/**
 * A class modelling foreign key relationships and constraints of tables in
 * DEFAULT_SCHEMA.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes", "this-escape" })
public class Keys {

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<EpisodesRecord> PK_EPISODES = Internal.createUniqueKey(Episodes.EPISODES, DSL.name("pk_episodes"), new TableField[] { Episodes.EPISODES.SERIES_ID, Episodes.EPISODES.SEASON_ID, Episodes.EPISODES.EPISODE_ID }, true);
    public static final UniqueKey<SeasonsRecord> PK_SEASONS = Internal.createUniqueKey(Seasons.SEASONS, DSL.name("pk_seasons"), new TableField[] { Seasons.SEASONS.SERIES_ID, Seasons.SEASONS.SEASON_ID }, true);
    public static final UniqueKey<SeriesRecord> PK_SERIES = Internal.createUniqueKey(Series.SERIES, DSL.name("pk_series"), new TableField[] { Series.SERIES.SERIES_ID }, true);
}
