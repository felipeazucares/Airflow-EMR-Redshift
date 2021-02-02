from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('project6_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow - project 6',
          schedule_interval='@hourly',
          catchup=True,
          # DEBUG this is here just to make debugging easier
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    # pass these context variables into the operator so we can backfill by day
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    json_path='s3://udacity-dend/log_json_path.json',
    context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path='auto',
    context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    append_mode=True,
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    append_mode=False,
    table='users',
    context=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    append_mode=False,
    table='songs',
    context=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    append_mode=False,
    table='artists',
    context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    append_mode=False,
    table='time',
    context=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.check_for_nulls,
    # table we will query
    table='songplays',
    # field we want to count nulls in
    field='playid',
    # what we should get as a result
    expected_result=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,
                   stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_user_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
