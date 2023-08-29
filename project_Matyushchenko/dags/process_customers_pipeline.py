from airflow import DAG
from customers_csv import customers_csv

from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

dag = DAG(
    dag_id="transfer_to_bigquery_customers",
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 5),
    schedule_interval='@daily',
    catchup=True,
    template_searchpath='/opt/airflow/bin_sql',
    max_active_runs=1
)

run_bigquery_job = BigQueryInsertJobOperator(
    task_id='loading_customers_dirty_bronze',
    dag=dag,
    location='US',
    project_id='project-matyushchenko-arthur',
    configuration={
        "query": {
            "query": "{% include 'parametrs_customers_dirty_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "customers_csv": customers_csv,
            }
        }
    },
    params={
        'project': 'project-matyushchenko-arthur',
        'data_lake_raw_bucket': 'final2023-de'
    }
)

run_bigquery_job2 = BigQueryInsertJobOperator(
    task_id='loading_customers_silver',
    dag=dag,
    location='US',
    project_id='project-matyushchenko-arthur',
    configuration={
        "query": {
            "query": "{% include 'parametrs_customers_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project': 'project-matyushchenko-arthur',
    }
)

run_bigquery_job >> run_bigquery_job2
