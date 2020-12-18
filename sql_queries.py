from concurrent.futures import ProcessPoolExecutor, as_completed
from json import loads
from typing import Iterable

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.pool import NullPool


with open("dwh.json") as cf:
    CONFIG = loads(cf.read())


def get_engine() -> Engine:
    """Provides an sqlalchemy engine.

    Returns:
        Engine
    """
    uri = "redshift+psycopg2://{user}:{passwd}@{host}:{port}/{db}".format(**CONFIG["redshift"])
    return create_engine(uri, echo=False, poolclass=NullPool, isolation_level="AUTOCOMMIT")


def run_query(query: str) -> str:
    """Runs a single query

    Args:
        query (str): Text of the query to run

    Returns:
        str: Returns the query text so que parallel runner can print it
    """
    eng = get_engine()
    eng.execute(query)
    return f"Completed: {query}"


def run_queries_sequential(queries: Iterable[str]):
    """Run queries in sequence

    Args:
        queries (Iterable[str]): A sequence of queries to run
    """
    for query in queries:
        run_query(query)
        print(f"Completed: {query}")


def run_queries_parallel(queries: Iterable[str]):
    """Run queries in parallel

    Args:
        queries (Iterable[str]):  A sequence of queries to run
    """
    with ProcessPoolExecutor(max_workers=4) as exec:
        query_futures = [exec.submit(run_query, query) for query in queries]
        for copy in as_completed(query_futures):
            print(copy.result())


# DROP TABLE STATEMENTS
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLE STATEMENTS
staging_events_table_create = """
CREATE TABLE staging_events (
    "artist" varchar,
    "auth" varchar,
    "firstName" varchar,
    "gender" varchar,
    "itemInSession" int,
    "lastName" varchar,
    "lengh" float,
    "level" varchar,
    "location" varchar,
    "method" varchar,
    "page" varchar,
    "registration" float,
    "sessionId" int,
    "song" varchar,
    "status" int,
    "ts" bigint,
    "userAgent" varchar,
    "userId" int
);
"""

staging_songs_table_create = """
CREATE TABLE staging_songs (
    "num_songs" int,
    "artist_id" varchar,
    "artist_latitude" float,
    "artist_longitude" float,
    "artist_location" varchar,
    "artist_name" varchar,
    "song_id" varchar,
    "title" varchar,
    "year" int,
    "duration" float
);
"""

songplay_table_create = """
CREATE TABLE songplays (
    "songplay_id" bigint IDENTITY(0,1),
    "start_time" timestamp NOT NULL sortkey distkey,
    "user_id" int NOT NULL,
    "level" varchar NULL,
    "song_id" varchar NOT NULL,
    "artist_id" varchar NOT NULL,
    "session_id" int NULL,
    "location" text NULL,
    "user_agent" text NULL,
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id),
    CONSTRAINT songplays_un UNIQUE (start_time, user_id, song_id),
    CONSTRAINT songplays_fk_2 FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT songplays_fk_3 FOREIGN KEY (song_id) REFERENCES songs(song_id),
    CONSTRAINT songplays_fk_4 FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);
"""

user_table_create = """
CREATE TABLE users (
    "user_id" int,
    "first_name" varchar NULL,
    "last_name" varchar NULL,
    "gender" char NULL,
    "level" varchar NULL,
    CONSTRAINT users_pk PRIMARY KEY (user_id)
) diststyle all;
"""

song_table_create = """
CREATE TABLE songs (
    "song_id" varchar,
    "title" varchar NOT NULL,
    "artist_id" varchar NOT NULL,
    "year" int NOT NULL,
    "duration" float NOT NULL,
    CONSTRAINT songs_pk PRIMARY KEY (song_id)
) diststyle all;
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
    "start_time" timestamp sortkey distkey,
    "hour" int NOT NULL,
    "day" int NOT NULL,
    "week" int NOT NULL,
    "month" int NOT NULL,
    "year" int NOT NULL,
    "weekday" int NOT NULL,
    CONSTRAINT time_pk PRIMARY KEY (start_time)
);
"""

# STAGING TABLE COPY STATEMENTS
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

# FINAL TABLES INSERT STATEMENTS
songplay_table_insert = """
INSERT INTO songplays
(start_time, user_id, "level", song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second',
        user_id,
        "level",
        songs.song_id,
        artists.artist_id,
        sessionid,
        location,
        useragent
FROM staging_events
INNER JOIN artists on artists.name = staging_events.artist
INNER JOIN songs on songs.title = staging_events.song
WHERE page='NextSong'
"""

user_table_insert = """
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, "level"
FROM staging_events
WHERE page='NextSong'
"""

song_table_insert = """
INSERT INTO songs
SELECT song_id, title, artist_id, year, duration
FROM staging_songs;
"""

artist_table_insert = """
INSERT INTO artists
SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM staging_songs
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