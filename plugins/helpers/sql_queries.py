class SqlQueries:
    create_dimension_table = ("""
        CREATE TABLE IF NOT EXISTS public.dimension_state (
        state_key varchar(2) NOT NULL,
        state_name varchar(256),
        average_age float8,
        female_urban_population bigint,
        male_urban_population bigint,
        total_urban_population bigint,
        CONSTRAINT state_key_pkey PRIMARY KEY(state_key))
    """)
    create_fact_table = ("""
        CREATE TABLE IF NOT EXISTS public.fact_arrivals ( 
        arrival_id bigint identity(0, 1),
        state_key varchar(2),
        month integer,
        average_age float8,
        F integer,
        M integer,
        U integer,
        X integer,
        business integer,
        pleasure integer,
        student integer,
        average_temperature float8,
        CONSTRAINT arrival_id_pkey PRIMARY KEY(arrival_id),
        CONSTRAINT state_key_fk
        FOREIGN KEY (state_key)
        REFERENCES dimension_state(state_key))
    """)
