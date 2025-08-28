CREATE TABLE app_token (
    id Text NOT NULL,
    username Text,
    version Int32,
    PRIMARY KEY (id)
) WITH (
    AUTO_PARTITIONING_BY_LOAD=ENABLED
);

CREATE TABLE app_token_log (
    id Text NOT NULL,
    global_version Int64 NOT NULl,
    token_id Text NOT NULL,
    updated_at Timestamp,
    updated_to Int32,
    PRIMARY KEY (id)
) WITH (
    AUTO_PARTITIONING_BY_LOAD=ENABLED
);
