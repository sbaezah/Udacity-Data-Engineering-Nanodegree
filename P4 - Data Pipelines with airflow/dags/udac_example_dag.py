"""
Udacity Data Engineer Nanodegree
4th project - data pipelines  with Airflow
Sebastian Baeza - October 2019
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, DataChecks

default_args = {
    'owner': 'sbaezah',
    'start_date': datetime(2019, 9, 23),
    'end_date': datetime(2019, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'schedule_interval': '@hourly'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          )


"""DAG schema components
Based on the project's requirements document, in this part, I declare the 
components of the DAG.
We will have two "Dummy" components; those Dummy components are the "Start" and "End" operators.
The other components allow us to extract, transform, and load our data. 
"""

#Dummy operators
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Extract - Transform - Load data to staging tables

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    data_format="json",
    jsonpaths="s3://udacity-dend/log_json_path.json",
    provide_context=True,
    dag=dag    
)


stage_songs_to_redshift = StageToRedshiftOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    ignore_headers="0",
    data_format="json",
    task_id='Stage_songs',
    provide_context=True,
    dag=dag
)

# Load data to fact and dimmension tables

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    destination_table="songplays",
    sql_statement=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    destination_table="users",
    sql_statement=SqlQueries.user_table_insert,
    update_mode="insert",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    destination_table="songs",
    sql_statement=SqlQueries.song_table_insert,
    update_mode="insert",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    destination_table="artists",
    sql_statement=SqlQueries.artist_table_insert,
    update_mode="insert",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    destination_table="time",
    sql_statement=SqlQueries.time_table_insert,
    update_mode="insert",
    dag=dag
)

# Quality check process
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    data_check_query=[DataChecks.check_empty_table,
                      DataChecks.check_empty_table,
                      DataChecks.check_empty_table,
                      DataChecks.check_songplay_id_duplicate],
    table=['staging_songs','staging_events', 'songplays', ""],
    expected_result=[1, 1, 1, 1],
    dag=dag
)


# Schema definition, this part allows watching the "Graph" view or "Tree" view of the DAG  

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator