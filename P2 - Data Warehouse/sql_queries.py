import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
                                                             event_id BIGINT IDENTITY(0,1),
                                                             artist_name VARCHAR(255),
                                                             auth VARCHAR(50),
                                                             user_first_name VARCHAR(255),
                                                             user_gender  VARCHAR(1),
                                                             item_in_session	INTEGER,
                                                             user_last_name VARCHAR(255),
                                                             song_length	DOUBLE PRECISION, 
                                                             user_level VARCHAR(50),
                                                             location VARCHAR(255),	
                                                             method VARCHAR(50),
                                                             page VARCHAR(50),	
                                                             registration VARCHAR(50),	
                                                             session_id	BIGINT,
                                                             song_title VARCHAR(255),
                                                             status INTEGER,
                                                             ts VARCHAR(50),
                                                             user_agent TEXT,	
                                                             user_id VARCHAR(255),
                                                             PRIMARY KEY (event_id)
                                                             )""")


staging_songs_table_create = ("""CREATE TABLE staging_songs(
                                                            song_id VARCHAR(100),
                                                            num_songs INTEGER,
                                                            artist_id VARCHAR(100),
                                                            artist_latitude DOUBLE PRECISION,
                                                            artist_longitude DOUBLE PRECISION,
                                                            artist_location VARCHAR(255),
                                                            artist_name VARCHAR(255),
                                                            title VARCHAR(255),
                                                            duration DOUBLE PRECISION,
                                                            year INTEGER,
                                                            PRIMARY KEY (song_id)
                                                            )""")


songplay_table_create = ( """CREATE TABLE songplays(
                                                            songplay_id BIGINT IDENTITY(0,1),
                                                            start_time TIMESTAMP REFERENCES time(start_time),
                                                            user_id VARCHAR(255) REFERENCES users(user_id),
                                                            level VARCHAR(50),
                                                            song_id VARCHAR(100) REFERENCES songs(song_id),
                                                            artist_id VARCHAR(100) REFERENCES artists(artist_id),
                                                            session_id BIGINT,
                                                            location VARCHAR(255),
                                                            user_agent TEXT,
                                                            PRIMARY KEY (songplay_id)
                                                            )""")

user_table_create = ("""CREATE TABLE users(
                                            user_id VARCHAR(255),
                                            first_name VARCHAR(255),
                                            last_name VARCHAR(255),
                                            gender VARCHAR(1),
                                            level VARCHAR(50),
                                            PRIMARY KEY (user_id)
                                            )""")

song_table_create = ("""CREATE TABLE songs(
                                            song_id VARCHAR(100),
                                            title VARCHAR(255),
                                            artist_id VARCHAR(100) NOT NULL,
                                            year INTEGER,
                                            duration DOUBLE PRECISION,
                                            PRIMARY KEY (song_id)
                                            )""")

artist_table_create = ("""CREATE TABLE artists(
                                                artist_id VARCHAR(100),
                                                name VARCHAR(255),
                                                location VARCHAR(255),
                                                latitude DOUBLE PRECISION,
                                                longitude DOUBLE PRECISION,
                                                PRIMARY KEY (artist_id)
                                                )""")

time_table_create = ("""CREATE TABLE time(
                                            start_time TIMESTAMP,
                                            hour INTEGER,
                                            day INTEGER,
                                            week INTEGER,
                                            month INTEGER,
                                            year INTEGER,
                                            weekday INTEGER,
                                            PRIMARY KEY (start_time))""")

# STAGING TABLES

# Loading from JSON Using a JSONPaths file (LOG_JSONPATH)


staging_events_copy = ("""copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON '{}'""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))



staging_songs_copy = ("""copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON 'auto'
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

# this is the only insert query that i made with a sub-query filter in the "WHERE" section
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        e.user_id, 
        e.user_level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    inner join staging_songs s on (e.song_title = s.title)
    WHERE e.page = 'NextSong'
    AND e.user_id NOT IN (SELECT DISTINCT s.user_id FROM songplays s WHERE s.user_id = user_id
                       AND s.start_time = start_time AND s.session_id = session_id )""")

# In the other insert tables queries i use a left join with null value search in the "WHERE" section
# this allow to discard the rows that already exists in the destination table

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
                        SELECT DISTINCT 
                            se.user_id,
                            se.user_first_name,
                            se.user_last_name,
                            se.user_gender, 
                            se.user_level
                        FROM staging_events se
                        LEFT JOIN users u on (se.user_id = u.user_id)
                        WHERE 
                            se.page = 'NextSong'
                            AND u.user_id is null
                        """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT
                            ss.song_id, 
                            ss.title,
                            ss.artist_id,
                            ss.year,
                            ss.duration
                            FROM staging_songs ss
                            LEFT JOIN songs s on (ss.song_id = s.song_id)
                            WHERE 
                                s.song_id IS NULL
                        """)


artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                            SELECT DISTINCT 
                                ss.artist_id,
                                ss.artist_name,
                                ss.artist_location,
                                ss.artist_latitude,
                                ss.artist_longitude
                            FROM staging_songs ss
                            LEFT JOIN artists a on ( ss.artist_id = a.artist_id )
                            WHERE 
                                a.artist_id IS NULL
                        """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT 
                            t1.start_time, 
                            EXTRACT(hr from t1.start_time) AS hour,
                            EXTRACT(d from t1.start_time) AS day,
                            EXTRACT(w from t1.start_time) AS week,
                            EXTRACT(mon from t1.start_time) AS month,
                            EXTRACT(yr from t1.start_time) AS year, 
                            EXTRACT(weekday from t1.start_time) AS weekday 
                        FROM (
                            SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
                            FROM staging_events s     
                        ) t1
                        LEFT JOIN time t2 on (t1.start_time = t2.start_time)
                        WHERE 
                            t2.start_time IS NULL""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,time_table_create ,user_table_create, song_table_create, artist_table_create,songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
