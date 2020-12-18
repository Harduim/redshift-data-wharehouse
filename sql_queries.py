from concurrent.futures import ProcessPoolExecutor, as_completed
from json import loads
from typing import Iterable

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.pool import NullPool


with open("dwh.json") as cf:
    CONFIG = loads(cf.read())


def get_engine() -> Engine:
    uri = "redshift+psycopg2://{user}:{passwd}@{host}:{port}/{db}".format(**CONFIG["redshift"])
    return create_engine(uri, echo=False, poolclass=NullPool, isolation_level="AUTOCOMMIT")


def run_query(query: str):
    eng = get_engine()
    eng.execute(query)
    return f"Completed: {query}"


def run_queries_sequential(queries: Iterable[str]):
    for query in queries:
        run_query(query)
        print(f"Completed: {query}")


def run_queries_parallel(queries: Iterable[str]):
    with ProcessPoolExecutor(max_workers=4) as exec:
        query_futures = [exec.submit(run_query, query) for query in queries]
        for copy in as_completed(query_futures):
            print(copy.result())


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE staging_events (
    "artist" varchar NULL,
    "auth" varchar NULL,
    "firstName" varchar NULL,
    "gender" varchar NULL,
    "itemInSession" int NULL,
    "lastName" varchar NULL,
    "lengh" varchar NULL,
    "level" varchar NULL,
    "location" varchar NULL,
    "method" varchar NULL,
    "page" varchar NULL,
    "registration" float NULL,
    "sessionId" varchar NULL,
    "song" varchar NULL,
    "status" int NULL,
    "ts" bigint NULL,
    "userAgent" varchar NULL,
    "userId" varchar NULL
);
"""

staging_songs_table_create = """
CREATE TABLE staging_songs (
    "num_songs" int NULL,
    "artist_id" varchar NULL,
    "artist_latitude" varchar NULL,
    "artist_longitude" varchar NULL,
    "artist_location" varchar NULL,
    "artist_name" varchar NULL,
    "song_id" varchar NULL,
    "title" varchar NULL,
    "year" int NULL,
    "duration" float NULL
);
"""

songplay_table_create = """
CREATE TABLE songplays (
    "songplay_id" bigint IDENTITY(0,1) NOT NULL,
    "start_time" timestamp NULL,
    "user_id" int NULL,
    "level" varchar NULL,
    "song_id" varchar NULL,
    "artist_id" varchar NULL,
    "session_id" int NULL,
    "location" text NULL,
    "user_agent" text NULL,
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id),
    CONSTRAINT songplays_un UNIQUE (start_time, user_id, song_id),
    CONSTRAINT songplays_fk_2 FOREIGN KEY (user_id) REFERENCES users(user_id)
);
"""

user_table_create = """
CREATE TABLE users (
    "user_id" int NOT NULL,
    "first_name" varchar NULL,
    "last_name" varchar NULL,
    "gender" char NULL,
    "level" varchar NULL,
    CONSTRAINT users_pk PRIMARY KEY (user_id)
);
"""

song_table_create = """
CREATE TABLE songs (
    "song_id" varchar NOT NULL,
    "title" varchar NOT NULL,
    "artist_id" varchar NOT NULL,
    "year" int NOT NULL,
    "duration" float NOT NULL,
    CONSTRAINT songs_pk PRIMARY KEY (song_id)
);
"""

artist_table_create = """
CREATE TABLE artists (
    "artist_id" varchar NOT NULL,
    "name" varchar NOT NULL,
    "location" varchar NULL,
    "latitude" float NULL,
    "longitude" float NULL,
    CONSTRAINT artists_pk PRIMARY KEY (artist_id)
);
"""

time_table_create = """
CREATE TABLE "time" (
    "start_time" timestamp NOT NULL,
    "hour" int NOT NULL,
    "day" int NOT NULL,
    "week" int NOT NULL,
    "month" int NOT NULL,
    "year" int NOT NULL,
    "weekday" int NOT NULL,
    CONSTRAINT time_pk PRIMARY KEY (start_time)
);
"""

# STAGING TABLES

events_copy = f"""
COPY staging_events FROM '{CONFIG["s3"]["log_data"]}'
iam_role '{CONFIG["iam_role"]}'
json '{CONFIG["s3"]["log_jsonpath"]}'
REGION 'us-west-2';
"""

songs_copy = f"""
COPY staging_songs FROM '{CONFIG["s3"]["song_data"]}'
iam_role '{CONFIG["iam_role"]}'
json 'auto'
REGION 'us-west-2';
"""

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays
(start_time, user_id, "level", song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + "ts"/1000 * INTERVAL '1 second',
        CAST("userid" as int) as user_id,
        "level",
        (
            SELECT song_id
            FROM songs
            INNER JOIN artists on songs.artist_id = artists.artist_id
            WHERE title = song and artist = artists."name"
        ),
        (SELECT artist_id FROM artists WHERE artist = artists."name"),
        CAST(sessionid AS INT),
        location,
        useragent
FROM staging_events as se
WHERE auth != 'Logged Out'
"""

user_table_insert = """
INSERT INTO users
SELECT cast("userId" as integer),
       max("firstName"),
       max("lastName"),
       max("gender"),
       max("level")
FROM staging_events
WHERE auth != 'Logged Out'
GROUP BY userid;
"""

song_table_insert = """
INSERT INTO songs
SELECT "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
FROM staging_songs;
"""

artist_table_insert = """
INSERT INTO artists
SELECT "artist_id",
        MAX("artist_name"),
        MAX("artist_location"),
        CAST(MAX("artist_latitude") as float),
        CAST(MAX("artist_longitude") as float)
FROM staging_songs
GROUP BY "artist_id"
"""

time_table_insert = """
INSERT INTO time
SELECT start_time,
        DATE_PART(HOUR, start_time),
        DATE_PART(DAY, start_time),
        DATE_PART(WEEK , start_time),
        DATE_PART(MONTH, start_time),
        DATE_PART(YEAR, start_time),
        DATE_PART(WEEKDAY, start_time)
FROM songplays
"""

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
insert_dim_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
]
staging_copy_queries = [events_copy, songs_copy]