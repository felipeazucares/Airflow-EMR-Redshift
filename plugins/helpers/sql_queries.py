class SqlQueries:

    create_dimension_table = ("""
        CREATE TABLE public.dimension_state (
        state_key varchar(2) NOT NULL,
        state_name varchar(256),
        average_age numeric(3,2),
        female_urban_population numeric(18,0),
        male_urban_population numeric(18,0),
        total_urban_population numeric(18,0),
        CONSTRAINT state_key_pkey PRIMARY KEY(state_key)
    """)
    # TODO need to put a foreign key ref in here for the state key
    create_fact_table = ("""
        arrival_id integer,
        state_key varchar(2) NOT NULL,
        month integer,
        average_age numeric(4,2),
        F integer,
        M integer,
        U integer,
        X integer,
        business numeric(18,0),
        pleasure numeric(18,0),
        student numeric(18,0),
        average_temperature numeric(4,2),
        CONSTRAINT arrival_id_pkey PRIMARY KEY(arrival_id)
    """)

    # Selects records that we will insert into songplays table
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)
    # Selects user records that we will insert into dimension table
    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)
    # Inserts songs into song table
    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    #
    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    check_for_nulls = ("""
        SELECT COUNT(*) 
        FROM {} WHERE {} IS NULL
    """)
