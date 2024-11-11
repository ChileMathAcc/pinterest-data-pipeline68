from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Path to Databricks notebook
notebook_task = {'notebook_path': '/Users/chilese@outlook.com/Pinterest Data Proj'}

# Parameters for the DatabricksRunNowOperator()
notebook_params = {"Variable": 6}

# Default parameter for the DAG
default_args = {
    'owner': '0affe94cc7d3',
    'depend_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 3)
}

with DAG('0affe94cc7d3_dag',
         start_date = days_ago(1),
         end_date = datetime(2024, 11, 29),
         schedule_interval = '@daily',
         catchup = False,
         default_args = default_args) as dag:
    
    # Define the task to be run
    opr_submit = DatabricksSubmitRunOperator(
        task_id = 'notebook_run',
        databricks_conn_id = 'databricks_default',
        existing_cluster_id = '1108-162752-8okw8dgg',
        notebook_task= notebook_task)
    
    # Run the task
    opr_submit