from sqlalchemy import create_engine, engine


def get_connection(redshift_cfg: dict) -> engine:
    print(redshift_cfg)
    uri = "redshift+psycopg2://{user}:{passwd}@{host}:{port}/{db}".format(**redshift_cfg)
    return create_engine(uri)


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS taging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE staging_events (
    "songplay_id" int NOT NULL,
    "start_time" timestamp NOT NULL,
    "user_id" int NOT NULL,
    "level" varchar NOT NULL
);
"""

staging_songs_table_create = """
CREATE TABLE songs (
    "song_id" varchar NOT NULL,
    "title" varchar NOT NULL,
    "artist_id" varchar NOT NULL,
    "year" int NOT NULL,
    "duration" float NOT NULL
);
"""

songplay_table_create = """
CREATE TABLE songplays (
    "songplay_id" IDENTITY(0,1) NOT NULL,
    "start_time" timestamp NOT NULL,
    "user_id" int NOT NULL,
    "level" varchar NOT NULL,
    "song_id" varchar NULL,
    "artist_id" varchar NULL,
    "session_id" int NOT NULL,
    "location" text NOT NULL,
    "user_agent" text NOT NULL,
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id),
    CONSTRAINT songplays_un UNIQUE (start_time, user_id, song_id),
    CONSTRAINT songplays_fk_2 FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT songplays_fk_3 FOREIGN KEY (start_time) REFERENCES "time"(start_time)
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

staging_events_copy = """
"""

staging_songs_copy = """
"""

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays
("start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent")
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
INSERT INTO users
("user_id", "first_name", "last_name", "gender", "level")
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
"""

song_table_insert = """
INSERT INTO songs
("song_id", "title", "artist_id", "year", "duration")
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
"""

artist_table_insert = """
INSERT INTO artists
("artist_id", "name", "location", "latitude", "longitude")
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
"""


time_table_insert = """
INSERT INTO "time"
("start_time", "hour", "day", "week", "month", "year", "weekday")
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
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
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
