class SqlQueries:
    create_dimension_table = ("""
        CREATE TABLE IF NOT EXISTS public.dimension_state (
        state_key varchar(2) NOT NULL,
        state_name varchar(256),
        average_age numeric(3,2),
        female_urban_population numeric(18,0),
        male_urban_population numeric(18,0),
        total_urban_population numeric(18,0),
        CONSTRAINT state_key_pkey PRIMARY KEY(state_key))
    """)
    create_fact_table = ("""
        CREATE TABLE IF NOT EXISTS public.fact_arrivals ( 
        arrival_id integer NOT NULL,
        state_key varchar(2),
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
        CONSTRAINT state_key_fk
        FOREIGN KEY (state_key_fk)
        REFERENCES dimension_state(state_key))
    """)
