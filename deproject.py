
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime,timedelta
import sys
path = "/home/vishal/Documents/DE"
sys.path.append(path)
from init import *
from rds_insertion import data_fetching_and_inserting
from athena import athena_fetching
default_args = {"owner":"vishal",'retries':5,'retry_delay':timedelta(minutes=2)}

with DAG(dag_id = "DEProjectDag",\
         description = "this is our first dag",\
         start_date  = datetime(2023,11,7),\
         schedule_interval="@daily",\
         end_date = datetime(2023,11,8),\
         default_args = default_args,\
         catchup=False) as dag:
    
    start = DummyOperator(task_id="start")

    task1 = PythonOperator(task_id="data-fetching-and-inserting",\
                           python_callable=data_fetching_and_inserting,\
                           op_kwargs={"url":url,"databaseconfig":databaseconfig,
                                      "queries":rds_queries,
                                      'error_log':error_log_location
                                      })
    
    task2 = SparkSubmitOperator(task_id= "spark",\
                                conn_id="DE_spark",\
                                application= spark_script,\
                                jars= ",".join(jars), \
                                application_args = ['--access_key',access_key,
                                                    '--secret_key',secret_key,
                                                    '--host',databaseconfig['host'],
                                                    '--db_name',databaseconfig['databaseName'],
                                                    '--user',databaseconfig['user'],
                                                    '--table',databaseconfig['table'],
                                                    '--pass',databaseconfig['password'],
                                                    '--s3outputlocation',s3sparklocation,
                                                    ],\
                                name = app_name
                                )
    
    task3 = PythonOperator(task_id="athena-fetching",python_callable=athena_fetching,\
                           op_kwargs={"access_key":access_key,
                                      "secret_key":secret_key,
                                      "databaseconfig":databaseconfig,
                                      's3config':s3config_athena,
                                      'Queries':athena_queries})
    end =   DummyOperator(task_id="end")
    start >> task1 >> task2 >> task3 >> end

