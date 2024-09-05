CREATE TABLE series
(
    series_id    Int64,
    title        Text,
    series_info  Text,
    release_date Date,
    PRIMARY KEY (series_id),
    INDEX        title_name GLOBAL ON (title)
);

CREATE TABLE seasons
(
    series_id   Int64,
    season_id   Int64,
    title       Text,
    first_aired Date,
    last_aired  Date,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE episodes
(
    series_id  Int64,
    season_id  Int64,
    episode_id Int64,
    title      Text,
    air_date   Date,
    PRIMARY KEY (series_id, season_id, episode_id)
);
