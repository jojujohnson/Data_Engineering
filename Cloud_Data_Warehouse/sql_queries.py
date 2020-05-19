import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# GLOBAL VARIABLES
S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")


# DROP TABLES
fact_songplays_drop = "DROP TABLE IF EXISTS fact_songplays"
dimension_users_drop = "DROP TABLE IF EXISTS dimension_users"
dimension_songs_drop = "DROP TABLE IF EXISTS dimension_songs"
dimension_artists_drop = "DROP TABLE IF EXISTS dimension_artists"
dimension_time_drop = "DROP TABLE IF EXISTS dimension_time"
staging_log_events_table_drop = "DROP TABLE IF EXISTS staging_log_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

# CREATE STAGING TABLES
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
);
""")

staging_log_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_log_events
(
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

#CREATE DIMENSION TABLES
dimension_users_create = ("""
CREATE TABLE IF NOT EXISTS dimension_users
(
user_id INTEGER PRIMARY KEY distkey,
first_name      VARCHAR,
last_name       VARCHAR,
gender          VARCHAR,
level           VARCHAR
);
""")

dimension_songs_create = ("""
CREATE TABLE IF NOT EXISTS dimension_songs
(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR,
artist_id   VARCHAR distkey,
year        INTEGER,
duration    FLOAT
);
""")

dimension_artists_create = ("""
CREATE TABLE IF NOT EXISTS dimension_artists
(
artist_id          VARCHAR PRIMARY KEY distkey,
name               VARCHAR,
location           VARCHAR,
latitude           FLOAT,
longitude          FLOAT
);
""")

dimension_time_create = ("""
CREATE TABLE IF NOT EXISTS dimension_time
(
start_time    TIMESTAMP PRIMARY KEY sortkey distkey,
hour          INTEGER,
day           INTEGER,
week          INTEGER,
month         INTEGER,
year          INTEGER,
weekday       INTEGER
);
""")

#CREATE FACT TABLE
fact_songplays_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
start_time           TIMESTAMP,
user_id              INTEGER,
level                VARCHAR,
song_id              VARCHAR,
artist_id            VARCHAR,
session_id           INTEGER,
location             VARCHAR,
user_agent           VARCHAR
);
""")


#LOAD STAGING TABLES S3 -> REDSHIFT
staging_log_events_copy = ("""
    copy staging_log_events 
    from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off
    format as json {}
    timeformat as 'epochmillisecs'
""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs 
    from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off
    format as json 'auto'
""").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)


#LOAD DIMENSION TABLES
dimension_users_insert = ("""
INSERT INTO dimension_users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_log_events
where userId IS NOT NULL;
""")

dimension_songs_insert = ("""
INSERT INTO dimension_songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

dimension_artists_insert = ("""
INSERT INTO dimension_artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

dimension_time_insert = ("""
INSERT INTO dimension_time(start_time, hour, day, week, month, year, weekday)
SELECT distinct ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM staging_log_events
WHERE ts IS NOT NULL;
""")

# LOAD FACT TABLES
fact_songplays_insert = ("""
INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
FROM staging_log_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")


# FETCH ROW COUNTS
row_count_staging_log_events = ("""
    SELECT COUNT(*) FROM staging_log_events
""")

row_count_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

row_count_fact_songplays = ("""
    SELECT COUNT(*) FROM fact_songplays
""")

row_count_dimension_users = ("""
    SELECT COUNT(*) FROM dimension_users
""")

row_count_dimension_songs = ("""
    SELECT COUNT(*) FROM dimension_songs
""")

row_count_dimension_artists = ("""
    SELECT COUNT(*) FROM dimension_artists
""")

row_count_dimension_time = ("""
    SELECT COUNT(*) FROM dimension_time
""")



# SQL OPERATIONS LIST
drop_table_sql_list = [staging_log_events_table_drop, staging_songs_table_drop, fact_songplays_drop, dimension_users_drop, dimension_songs_drop, dimension_artists_drop, dimension_time_drop]

create_table_sql_list = [staging_log_events_table_create, staging_songs_table_create, fact_songplays_create, dimension_users_create, dimension_songs_create, dimension_artists_create, dimension_time_create]

staging_table_sql_list = [staging_log_events_copy, staging_songs_copy]

insert_table_sql_list = [fact_songplays_insert, dimension_users_insert, dimension_songs_insert, dimension_artists_insert, dimension_time_insert]

row_count_sql_list= [row_count_staging_log_events, row_count_staging_songs, row_count_fact_songplays, row_count_dimension_users, row_count_dimension_songs, row_count_dimension_artists, row_count_dimension_time]