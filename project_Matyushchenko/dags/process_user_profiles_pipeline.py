from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Константи для полів схеми
EMAIL = "email"
FULL_NAME = "full_name"
STATE = "state"
BIRTH_DATE = "birth_date"
PHONE_NUMBER = "phone_number"
STRING = "STRING"
REQUIRED = "REQUIRED"
DATE = "DATE"

# Константи для інших параметрів
BUCKET_NAME = 'final2023-de'
DESTINATION_PROJECT_DATASET_TABLE = 'project-matyushchenko-arthur.silver.user_profiles'

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 10),
}

dag = DAG(
    'load_jsonl',
    default_args=default_args
)

data_schema = [
    {"name": EMAIL, "type": STRING, "mode": REQUIRED},
    {"name": FULL_NAME, "type": STRING, "mode": REQUIRED},
    {"name": STATE, "type": STRING, "mode": REQUIRED},
    {"name": BIRTH_DATE, "type": DATE, "mode": REQUIRED},
    {"name": PHONE_NUMBER, "type": STRING, "mode": REQUIRED}
]

load_jsonl_to_bigquery = GCSToBigQueryOperator(
    task_id='load_jsonl_to_bigquery',
    dag=dag,
    bucket=BUCKET_NAME,
    location='US',
    source_objects=['raw/user_profiles/*.jsonl'],
    destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_EMPTY',
    schema_fields=data_schema,
)
