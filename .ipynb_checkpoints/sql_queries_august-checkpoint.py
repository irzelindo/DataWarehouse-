import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES


staging_events_table_create= (""" 
    CREATE TABLE staging_events (
        artist text,
        auth text,
        firstName text,
        gender text,
        itemInSession text,
        lastName text,
        length text,
        level text,
        location text,
        method text,
        page text,
        registration text,
        sessionID int,
        song text,
        status text,
        ts bigint,
        userAgent text,
        user_id int
    )
""")

staging_songs_table_create = (""" 
    CREATE TABLE staging_songs (
        num_songs int,
        artist_id text,
        artist_latitude numeric,
        artist_longitude numeric,    
        artist_location text,
        artist_name text,
        song_id text,
        title text,
        duration numeric,
        year int
    )
""")


songplay_table_create =("""
    CREATE TABLE songplay_table (
        songplay_id int IDENTITY(0,1) PRIMARY KEY, 
        start_time timestamp NOT NULL,  
        user_id int,  
        level text, 
        song_id text,  
        artist_id text,  
        session_id int,  
        location text,  
        user_agent text
    )
""")


user_table_create = (""" 
    CREATE TABLE user_table (
        user_id int PRIMARY KEY, 
        first_name text, 
        last_name text, 
        gender text, 
        level text
        )
""")


song_table_create = (""" 
    CREATE TABLE song_table (
        song_id text NOT NULL PRIMARY KEY, 
        title text, 
        artist_id text, 
        year int, 
        duration numeric
    )
""")


artist_table_create = (""" 
    CREATE TABLE artist_table (
        artist_id text NOT NULL PRIMARY KEY, 
        name text, 
        location text, 
        latitude numeric, 
        longitude numeric
    )
""")


time_table_create = (""" 
    CREATE TABLE time_table (
        start_time timestamp NOT NULL PRIMARY KEY, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday text
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
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = (""" 
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT events.start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.sessionID, events.location, events.userAgent 
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page = 'NextSong') events
    LEFT JOIN staging_songs songs
    ON events.song = songs.title
    AND events.artist = songs.artist_name
    AND events.length = songs.duration
""")


user_table_insert = (""" 
    INSERT INTO user_table (user_id, first_name, last_name, gender, level) 
    SELECT DISTINCT user_id, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
""") 

song_table_insert = (""" 
    INSERT INTO song_table (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
""")
 
artist_table_insert = (""" 
    INSERT INTO artist_table (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = (""" 
    INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, 
    EXTRACT(hour FROM start_time), 
    EXTRACT(day FROM start_time), 
    EXTRACT(week FROM start_time), 
    EXTRACT(year FROM start_time), 
    EXTRACT(month FROM start_time),
    EXTRACT(weekday FROM start_time)
    FROM songplay_table
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]