
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#------------------------------------------------------------------------------------------------------------------------------------------
#Statements used to delete stage tables, fact tables, dim tables before restoring them

staging_events_table_drop = "DROP TABLE IF EXISTS public.staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS public.songplays"
user_table_drop = "DROP TABLE IF EXISTS public.users"
song_table_drop = "DROP TABLE IF EXISTS public.songs"
artist_table_drop = "DROP TABLE IF EXISTS public.artists"
time_table_drop = "DROP TABLE  IF EXISTS public.time"

#------------------------------------------------------------------------------------------------------------------------------------------
# CREATE STAGE TABLES
#Create stage tables, which are used to store data extracted from AWS S3

staging_events_table_create= ("""
            CREATE TABLE public.staging_events (
                    artist varchar(256),
                    auth varchar(256),
                    firstname varchar(256),
                    gender varchar(256),
                    iteminsession int4,
                    lastname varchar(256),
                    length numeric(18,0),
                    "level" varchar(256),
                    location varchar(256),
                    "method" varchar(256),
                    page varchar(256),
                    registration numeric(18,0),
                    sessionid int4,
                    song varchar(256),
                    status int4,
                    ts int8,
                    useragent varchar(256),
                    userid int4
                );
            """)

staging_songs_table_create = ("""

            CREATE TABLE public.staging_songs (
                num_songs int4,
                artist_id varchar(256),
                artist_name varchar(256),
                artist_latitude numeric(18,0),
                artist_longitude numeric(18,0),
                artist_location varchar(256),
                song_id varchar(256),
                title varchar(256),
                duration numeric(18,0),
                "year" int4
);
            """)

songplay_table_create = ("""
            CREATE TABLE public.songplays (
                    playid varchar(32) NOT NULL,
                    start_time timestamp NOT NULL,
                    userid int4 NOT NULL,
                    "level" varchar(256),
                    songid varchar(256),
                    artistid varchar(256),
                    sessionid int4,
                    location varchar(256),
                    user_agent varchar(256),
                    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
                );

                """)

user_table_create = ("""
            CREATE TABLE public.users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
);
            
            """)

song_table_create = ("""
            CREATE TABLE public.songs (
                    songid varchar(256) NOT NULL,
                    title varchar(256),
                    artistid varchar(256),
                    "year" int4,
                    duration numeric(18,0),
                    CONSTRAINT songs_pkey PRIMARY KEY (songid)
                );
            """)

artist_table_create = ("""
            CREATE TABLE public.artists (
                    artistid varchar(256) NOT NULL,
                    name varchar(256),
                    location varchar(256),
                    lattitude numeric(18,0),
                    longitude numeric(18,0)
            );""")

time_table_create = ("""
            CREATE TABLE public."time" (
                start_time timestamp NOT NULL,
                "hour" int4,
                "day" int4,
                week int4,
                "month" varchar(256),
                "year" int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
                );
            """)
#------------------------------------------------------------------------------------------------------------------------------------------

# ARN             = config.get('IAM_ROLE', 'ARN')
# LOG_DATA        = config.get('S3', 'LOG_DATA')
# LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
# SONG_DATA       = config.get('S3', 'SONG_DATA')
# KEY =  config.get('AWS', 'KEY')
# SECRET =  config.get('AWS', 'SECRET')

#Execute command  COPY data from AWS S3 to stage tables
# staging_events_copy = ("""               
#             COPY staging_events
#             FROM {}
#             credentials 'aws_iam_role={}'                    
#             REGION 'us-west-2'
#             JSON {}
#             COMPUPDATE OFF;
#         """).format(LOG_DATA, ARN, LOG_JSONPATH)


# staging_songs_copy = ("""
#             COPY staging_songs FROM {}
#             credentials 'aws_iam_role={}'
#             JSON  'auto'
#             ACCEPTINVCHARS AS '^'
#             STATUPDATE ON
#             region 'us-west-2';
#         """).format(SONG_DATA, ARN)

#------------------------------------------------------------------------------------------------------------------------------------------
# From stage tables, perform INSERT on fact tables, dim table with project conditions

# songplay_table_insert = ("""
#             CREATE TABLE public.songplays (
#                     playid varchar(32) NOT NULL,
#                     start_time timestamp NOT NULL,
#                     userid int4 NOT NULL,
#                     "level" varchar(256),
#                     songid varchar(256),
#                     artistid varchar(256),
#                     sessionid int4,
#                     location varchar(256),
#                     user_agent varchar(256),
#                     CONSTRAINT songplays_pkey PRIMARY KEY (playid)
#                 );
#             """)




# user_table_insert = ("""INSERT INTO users (userId, firstName, lastName, gender, level)
#                         SELECT  DISTINCT SE.userId, SE.firstName, SE.lastName, SE.gender,SE.level  
#                         FROM staging_events as SE
#                         WHERE SE.page = 'NextSong' and SE.userId IS NOT NULL""")

# song_table_insert = ("""INSERT INTO songs(songId, title, artistId, year, duration)
#                         SELECT  DISTINCT SS.song_id, SS.title, SS.artist_id, SS.year, SS.duration 
#                         FROM staging_songs as SS
#                         WHERE SS.song_id is nOT NULL""")


# artist_table_insert = ("""CREATE TABLE public.artists (
#                                 artistid varchar(256) NOT NULL,
#                                 name varchar(256),
#                                 location varchar(256),
#                                 lattitude numeric(18,0),
#                                 longitude numeric(18,0)
#                         );""")


# time_table_insert = ("""INSERT INTO time (startTime, hour, day, week, month, year, weekday)
#                         SELECT  DISTINCT TIMESTAMP 'epoch' + (SE.ts/1000) * INTERVAL '1 second' as startTime ,
#                                             EXTRACT(hour FROM startTime),
#                                             EXTRACT(day FROM startTime),
#                                             EXTRACT(week FROM startTime),
#                                             EXTRACT(month FROM startTime),
#                                             EXTRACT(year FROM startTime),
#                                             EXTRACT(week FROM startTime) 
#                         FROM staging_events as SE
#                         """)





#------------------------------------------------------------------------------------------------------------------------------------------
# QUERY LISTS


create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



