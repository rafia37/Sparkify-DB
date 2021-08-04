# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY,
                                                          first_name varchar,
                                                          last_name varchar,
                                                          gender varchar,
                                                          level varchar);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY,
                                                            name varchar,
                                                            location varchar,
                                                            latitude numeric,
                                                            longitude numeric);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY,
                                                          title varchar,
                                                          artist_id varchar REFERENCES artists (artist_id),
                                                          year int,
                                                          duration numeric);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time time,
                                                         hour numeric,
                                                         day numeric,
                                                         week numeric,
                                                         month int,
                                                         year int,
                                                         weekday varchar);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int PRIMARY KEY, 
                                                                  start_time timestamp, 
                                                                  user_id int REFERENCES users (user_id), 
                                                                  level varchar, 
                                                                  song_id varchar REFERENCES songs (song_id), 
                                                                  artist_id varchar REFERENCES artists (artist_id),
                                                                  session_id int,
                                                                  location varchar,
                                                                  user_agent varchar);
""")


# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
                             ON CONFLICT (songplay_id) \
                             DO NOTHING
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
                             VALUES (%s, %s, %s, %s, %s) \
                             ON CONFLICT (user_id) \
                             DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                             VALUES (%s, %s, %s, %s, %s) \
                             ON CONFLICT (song_id) \
                             DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                             VALUES (%s, %s, %s, %s, %s) \
                             ON CONFLICT (artist_id) \
                             DO NOTHING
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                             VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, songs.artist_id \
                    FROM songs JOIN artists \
                        ON songs.artist_id = artists.artist_id \
                    WHERE title = %s AND \
                        name = %s AND \
                        duration = %s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]