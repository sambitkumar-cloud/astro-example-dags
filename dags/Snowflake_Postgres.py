from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.copy import SnowflakeToPostgresOperator

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'snowflake_to_postgres',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

copy_data = SnowflakeToPostgresOperator(
    task_id='copy_data',
    snowflake_conn_id='snowflake_default',
    postgres_conn_id='postgres_default',
    sql='SELECT * FROM snowflake_schema.table_name',
    target_table='postgres_schema.table_name',
    dag=dag
)
