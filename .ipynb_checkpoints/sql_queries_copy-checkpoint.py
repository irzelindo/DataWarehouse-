import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

songplay_table_drop = "DROP TABLE IF EXISTS songsplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = " DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create = (""" 
    CREATE TABLE IF NOT EXISTS staging_events (
        userId integer PRIMARY KEY,
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession integer,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method text,
        page varchar,
        registration float,
        sessionId varchar,
        song varchar,
        status integer,
        ts integer,
        userAgent varchar
    )
""")

staging_songs_table_create = (""" 
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id varchar PRIMARY KEY,
        num_songs integer,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year integer
    )
""")


songplay_table_create = (""" 
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int identity(0,1) PRIMARY KEY,
        song_id varchar,
        start_time timestamp,
        userId integer,
        level varchar,
        artist_id varchar,
        sessionId varchar,
        location varchar,
        userAgent varchar
    )
""")

user_table_create = (""" 
    CREATE TABLE IF NOT EXISTS users (
        userId integer PRIMARY KEY NOT NULL,
        firstName varchar,
        lastName varchar,
        gender varchar,
        level varchar
    )
""")


song_table_create = (""" 
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar primary key,
        title varchar,
        artist_id varchar,
        year integer,
        duration integer
    )
""")


artist_table_create = (""" 
    CREATE TABLE artists (
        artist_id varchar primary key,
        artist_name varchar,
        artist_location varchar,
        artist_latitude numeric,
        artist_longitude numeric
    )
""")


time_table_create = (""" 
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL primary key,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday varchar
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])




# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

