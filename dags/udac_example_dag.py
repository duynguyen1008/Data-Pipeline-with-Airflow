from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
              'udac_example_dag',
              default_args=default_args,
              schedule_interval='@hourly',
              catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = "staging_events",
    redshift_conn_id = "redshift",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/",
    aws_credentials_id = "aws_credentials",
    region = "us-west-2",
    provide_context = False,
    data_format = "JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    s3_bucket ="udacity-dend",
    s3_key = "song_data/",
    aws_credentials_id = "aws_credentials",
    region = "us-west-2",
    provide_context = False,
    data_format = "JSON"
    
)
#--------------------------------------------------------------------------
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "public.songplays",
    sql_query = SqlQueries.songplay_table_insert,
    append_only = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "public.users",
    sql_query = SqlQueries.user_table_insert,
    append_only = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "public.songs",
    sql_query = SqlQueries.song_table_insert,
    append_only = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "public.artists",
    sql_query = SqlQueries.artist_table_insert,
    append_only = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "public.time",
    sql_query = SqlQueries.time_table_insert,
    append_only = False
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = "redshift",
    check_datas = ["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table


load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
