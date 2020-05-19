
# DROP TABLES
fact_songplays_drop = "DROP TABLE IF EXISTS public.staging_songs"
dimension_users_drop = "DROP TABLE IF EXISTS public.staging_events"
dimension_songs_drop = "DROP TABLE IF EXISTS public.users"
dimension_artists_drop = "DROP TABLE IF EXISTS public.songs"
dimension_time_drop = "DROP TABLE IF EXISTS public.artists "
staging_log_events_table_drop = "DROP TABLE IF EXISTS public.time"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.songplays"

# CREATE STAGING TABLES
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_songs (
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

staging_log_events_table_create= ("""
CREATE TABLE IF NOT EXISTS public.staging_events (
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

#CREATE DIMENSION TABLES
dimension_users_create = ("""
CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
""")

dimension_songs_create = ("""
CREATE TABLE IF NOT EXISTS public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
""")

dimension_artists_create = ("""
CREATE TABLE IF NOT EXISTS public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);
""")

dimension_time_create = ("""
CREATE TABLE IF NOT EXISTS public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	dayofweek varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);
""")

#CREATE FACT TABLE
fact_songplays_create = ("""
CREATE TABLE IF NOT EXISTS public.songplays (
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


drop_table_sql_list = [staging_log_events_table_drop, staging_songs_table_drop, fact_songplays_drop, dimension_users_drop, dimension_songs_drop, dimension_artists_drop, dimension_time_drop]

create_table_sql_list = [staging_log_events_table_create, staging_songs_table_create, fact_songplays_create, dimension_users_create, dimension_songs_create, dimension_artists_create, dimension_time_create]















