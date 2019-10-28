class SqlQueries:
    """Insert query statements for the Amazon Redshift Cluster for DAG"""
    songplay_table_insert = ("""
                                SELECT
                                        md5(song_id || CAST(stg_events.start_time as VARCHAR) || nvl(CAST(stg_events.userid as VARCHAR), '-NOUSER-')) as songplay_id,
                                        stg_events.start_time, 
                                        nvl(stg_events.userid, -9999), 
                                        stg_events.level, 
                                        songs.song_id, 
                                        songs.artist_id, 
                                        stg_events.sessionid, 
                                        stg_events.location, 
                                        stg_events.useragent
                                FROM (  SELECT      TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                                                    *
                                        FROM staging_events
                                        WHERE page='NextSong'
                                     ) stg_events
                                LEFT JOIN staging_songs songs ON  ( stg_events.song = songs.title
                                                                    AND stg_events.artist = songs.artist_name
                                                                    AND stg_events.length = songs.duration)
                                WHERE 
                                        stg_events.start_time IS NOT NULL 
                                    AND songs.song_id IS NOT NULL
                             """)

    user_table_insert = ("""
                            SELECT distinct 
                                    stg_events.userid,
                                    stg_events.firstname,
                                    stg_events.lastname,
                                    stg_events.gender,
                                    stg_events.level
                            FROM staging_events stg_events
                            WHERE stg_events.page='NextSong'
                                  AND stg_events.userid IS NOT NULL
                        """)

    song_table_insert = ("""
                            SELECT distinct 
                                    stg_songs.song_id,
                                    stg_songs.title,
                                    stg_songs.artist_id,
                                    stg_songs.year,
                                    stg_songs.duration
                            FROM    staging_songs stg_songs
                            WHERE
                                    stg_songs.song_id IS NOT NULL
                        """)

    artist_table_insert = ("""
                                SELECT distinct
                                        stg_songs.artist_id,
                                        stg_songs.artist_name,
                                        stg_songs.artist_location,
                                        stg_songs.artist_latitude,
                                        stg_songs.artist_longitude
                                FROM staging_songs stg_songs
                                WHERE 
                                     stg_songs.artist_id IS NOT NULL
                          """)

    time_table_insert = ("""
                            SELECT 
                                    spl.start_time,
                                    extract(hour from spl.start_time),
                                    extract(day from spl.start_time), 
                                    extract(week from spl.start_time), 
                                    extract(month from spl.start_time),
                                    extract(year from spl.start_time),
                                    extract(dayofweek from spl.start_time)
                            FROM songplays spl
                            WHERE 
                                    spl.start_time IS NOT NULL
                        """)
    