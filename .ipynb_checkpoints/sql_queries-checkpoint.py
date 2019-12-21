'''
    This file contains all the SQL queries required to create & drop schemas and tables,
    Copy/Extract data from Udacity S3 bucket, transform and load that data into the 
    dimension and fact tables
'''
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE SCHEMAS
fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables")

# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_tables.events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_tables.songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_tables.songplays"
user_table_drop = "DROP TABLE IF EXISTS dimension_tables.users"
song_table_drop = "DROP TABLE IF EXISTS dimension_tables.songs"
artist_table_drop = "DROP TABLE IF EXISTS dimension_tables.artists"
time_table_drop = "DROP TABLE IF EXISTS dimension_tables.time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_tables.events (
        event_id int IDENTITY(0,1) PRIMARY KEY,
        artist text,
        auth text,
        firstname text,
        gender text,
        iteminsession int,
        lastname text,
        length numeric,
        level text,
        location text,
        method text,
        page text,
        registration numeric,
        session_id int,
        song text,
        status int,
        ts bigint,
        useragent text,
        user_id int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_tables.songs (
        song_id int IDENTITY(0,1) PRIMARY KEY,
        num_songs int,
        artist_id text,
        artist_name text,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location text,
        song_id text,
        title text,
        duration numeric,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE fact_tables.songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id integer NOT NULL,
        level text,
        song_id text,
        artist_id text,
        session_id integer,
        location text,
        user_agent text
    )
""")

user_table_create = ("""
    CREATE TABLE dimension_tables.users (
        user_id integer PRIMARY KEY,
        first_name text,
        last_name text,
        gender text,
        level text
    )
""")

song_table_create = ("""
    CREATE TABLE dimension_tables.songs (
        song_id text PRIMARY KEY,
        title text,
        artist_id text,
        year integer,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE dimension_tables.artists (
        artist_id text PRIMARY KEY,
        name text,
        location text,
        lattitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE dimension_tables.time (
        start_time timestamp PRIMARY KEY,
        hour integer,
        day integer,
        week integer,
        month text,
        year integer,
        weekday text
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_tables.events
    from {}
    iam_role {}
    json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    copy staging_tables.songs
    from {}
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_tables.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT events.start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.session_id, events.location, events.useragent
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
          FROM staging_tables.events
          WHERE page='NextSong') events
    LEFT JOIN staging_tables.songs songs
    ON events.song = songs.title
    AND events.artist = songs.artist_name
    AND events.length = songs.duration
""")

user_table_insert = ("""
    INSERT INTO dimension_tables.users (user_id, first_name, last_name, gender, level)
    SELECT distinct user_id, firstname, lastname, gender, level
    FROM staging_tables.events
    WHERE page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO dimension_tables.songs (song_id, title, artist_id, year, duration)
    SELECT distinct song_id, title, artist_id, year, duration
    FROM staging_tables.songs
""")

artist_table_insert = ("""
    INSERT INTO dimension_tables.artists (artist_id, name, location, lattitude, longitude)
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_tables.songs
""")

time_table_insert = ("""
    INSERT INTO dimension_tables.time (start_time, hour, day, week, month, year, weekday)
    SELECT distinct TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, extract(hour from start_time), extract(day from start_time),extract(week from start_time), 
    extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
    FROM staging_tables.events
""")

# QUERY LISTS

create_schema_queries = [fact_schema, dimension_schema, staging_schema]
drop_schema_queries = [fact_schema_drop, dimension_schema_drop, staging_schema_drop]

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
