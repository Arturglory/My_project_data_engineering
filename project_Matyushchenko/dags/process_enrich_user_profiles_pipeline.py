from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bigquery_data_pipeline',
    default_args=default_args,
    description='DAG для обрабки в BigQuery',
    schedule_interval=None,
    template_searchpath='/opt/airflow/bin_sql'
)

run_bigquery_job1 = BigQueryInsertJobOperator(
    task_id='creating_process_user_profiles_gold',
    dag=dag,
    location='US',
    project_id='project-matyushchenko-arthur',
    configuration={
        "query": {
            "query": "{% include 'create_table_enrich_user.sql' %}",
            "useLegacySql": False,
        }}
)

run_bigquery_job2 = BigQueryInsertJobOperator(
    task_id='loading_process_user_profiles_gold',
    dag=dag,
    location='US',
    project_id='project-matyushchenko-arthur',
    configuration={
        "query": {
            "query": "{% include 'insert_data_enrich_user.sql' %}",
            "useLegacySql": False,
        }}
)
run_bigquery_job1 >> run_bigquery_job2
