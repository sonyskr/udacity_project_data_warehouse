import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP table if exists staging_events"
staging_songs_table_drop = "DROP table if exists staging_songs"
songplay_table_drop = "DROP table if exists songplays"
user_table_drop = "DROP table if exists users"
song_table_drop = "DROP table if exists songs"
artist_table_drop = "DROP table if exists artists"
time_table_drop = "DROP table if exists time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events 
(
    artist        VARCHAR,
    auth          VARCHAR,
    firstName     VARCHAR(50),
    gender        VARCHAR,
    itemInSession INT,
    lastName      VARCHAR(50),
    length        FLOAT,
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  BIGINT,
    sessionid     INT,
    song          VARCHAR,
    status        INT,
    ts            BIGINT,
    userAgent     VARCHAR,
    userId        INT
    )

""")

staging_songs_table_create = ("""CREATE TABLE staging_songs 
(
    num_songs        INT,
    artist_id        VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         FLOAT,
    year             INT

)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (

   songplay_id          INTEGER     IDENTITY (0,1),
   start_time           TIMESTAMP,
   user_id              INT NOT NULL,
   level                VARCHAR,
   song_id              VARCHAR,
   artist_id            VARCHAR,
   session_id           INT,
   location             VARCHAR,
   user_agent           VARCHAR

)

""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(

   user_id       INT PRIMARY KEY,
   first_name    VARCHAR(50),
   last_name     VARCHAR(50),
   gender        VARCHAR,
   level         VARCHAR

)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(

   song_id      VARCHAR PRIMARY KEY,
   title        VARCHAR,
   artist_id    VARCHAR,
   year         INT,
   duration     FLOAT
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(

   artist_id    VARCHAR PRIMARY KEY,
   name         VARCHAR,
   location     VARCHAR,
   latitude     FLOAT,
   longitude    FLOAT
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(

   start_time      TIMESTAMP PRIMARY KEY,
   hour            INT NOT NULL,
   day             INT NOT NULL,
   week            INT NOT NULL,
   month           INT NOT NULL,
   year            INT NOT NULL,
   weekday         VARCHAR(9) NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""

copy staging_events

from {}

iam_role {}

json {};

""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs

from {}

iam_role {}

json 'auto';

""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
            se.userId,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId,
            se.location,
            se.userAgent
    FROM staging_songs ss
    INNER JOIN staging_events se
    ON (ss.title = se.song AND se.artist = ss.artist_name)
    AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time
SELECT DISTINCT
    TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
