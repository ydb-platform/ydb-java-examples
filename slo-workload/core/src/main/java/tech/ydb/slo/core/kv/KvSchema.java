package tech.ydb.slo.core.kv;

public final class KvSchema {
    public static final String CREATE_TABLE_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `%s` ("
            + "  hash Uint64,"
            + "  id Uint64,"
            + "  payload_str Utf8,"
            + "  payload_double Double,"
            + "  payload_timestamp Timestamp,"
            + "  payload_hash Uint64,"
            + "  PRIMARY KEY (hash, id)"
            + ") WITH ("
            + "  UNIFORM_PARTITIONS = %d,"
            + "  AUTO_PARTITIONING_BY_SIZE = ENABLED,"
            + "  AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,"
            + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,"
            + "  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d"
            + ")";

    public static final String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS `%s`";

    public static final String UPSERT_TEMPLATE = ""
            + "UPSERT INTO `%s` ("
            + "  hash, id, payload_str, payload_double, payload_timestamp, payload_hash"
            + ") VALUES (?, ?, ?, ?, ?, ?)";

    public static final String SELECT_TEMPLATE = ""
            + "SELECT id, payload_str, payload_double, payload_timestamp, payload_hash"
            + "  FROM `%s`"
            + "  WHERE id = ? AND hash = ?";

    private KvSchema() {
    }
}
