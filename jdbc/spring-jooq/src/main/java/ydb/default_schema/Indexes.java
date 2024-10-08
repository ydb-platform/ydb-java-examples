/*
 * This file is generated by jOOQ.
 */
package ydb.default_schema;


import org.jooq.Index;
import org.jooq.OrderField;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;

import ydb.default_schema.tables.Series;


/**
 * A class modelling indexes of tables in DEFAULT_SCHEMA.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes", "this-escape" })
public class Indexes {

    // -------------------------------------------------------------------------
    // INDEX definitions
    // -------------------------------------------------------------------------

    public static final Index TITLE_NAME = Internal.createIndex(DSL.name("title_name"), Series.SERIES, new OrderField[] { Series.SERIES.TITLE }, false);
}
