# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INT PRIMARY KEY,
                                                            first_name VARCHAR,
                                                            last_name VARCHAR,
                                                            gender VARCHAR,
                                                            level VARCHAR
                                                            );""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar NOT NULL PRIMARY KEY,
                                                            title varchar,
                                                            artist_id varchar, 
                                                            year int,
                                                            duration NUMERIC(10,5)
                                                            );""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR NOT NULL PRIMARY KEY,
                                                                name VARCHAR,
                                                                location VARCHAR,
                                                                latitude DECIMAL,
                                                                longitude DECIMAL
                                                                );""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP NOT NULL PRIMARY KEY,
                                                        hour INT,
                                                        day INT,
                                                        week INT,
                                                        month INT,
                                                        year INT,
                                                        weekday VARCHAR
                                                        );""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id BIGSERIAL PRIMARY KEY,
                                                                    start_time timestamp NOT NULL,
                                                                    user_id int NOT NULL REFERENCES users(user_id),
                                                                    level varchar,
                                                                    song_id varchar REFERENCES songs(song_id),
                                                                    artist_id varchar REFERENCES artists(artist_id),
                                                                    session_id int,
                                                                    location varchar,
                                                                    user_agent varchar
                                                                    );""")

# INSERT RECORDS

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level) 
                        values(%s,%s,%s,%s,%s) 
                        ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs(song_id,title,artist_id,year,duration) 
                        VALUES (%s, %s, %s, %s, %s) 
                        ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location,latitude,longitude) 
                            VALUES (%s, %s, %s, %s,%s) 
                            ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""INSERT INTO time(start_time,hour,day,week,month,year,weekday) 
                        values(%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (start_time) DO NOTHING
""")

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) 
                            values(%s,%s,%s,%s,%s,%s,%s,%s)
""")

# FIND SONGS

song_select = ("""select a1.song_id,a2.artist_id 
                    from songs a1 
                    join artists a2 on a1.artist_id = a2.artist_id 
                    where 
                    a1.title=(%s) and 
                    a2.name=(%s) and 
                    a1.duration=(%s);
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]