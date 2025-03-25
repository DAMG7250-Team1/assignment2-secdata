# print("Loading DAG file...")

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from datetime import datetime, timedelta
# from test_connections import test_connections
# from raw import download_sec_data, upload_to_s3, process_and_load_to_snowflake
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# import logging
# from airflow.hooks.base import BaseHook

# import sys
# print(sys.path)
# print("Loading DAG file...")

# logger = logging.getLogger(__name__)

# # Define constants
# DBT_PROJECT_DIR = "/opt/airflow/dags/financial_dbt_project"
# DBT_PROFILES_DIR = "/opt/airflow/dags/financial_dbt_project"

# aws_conn = BaseHook.get_connection("aws_default")
# aws_access_key = aws_conn.login
# aws_secret_key = aws_conn.password

# # Define DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     'sec_data_pipeline',
#     default_args=default_args,
#     description='Download SEC financial statement data and upload to S3',
#     schedule_interval=None,
#     catchup=False
# ) as dag:

#     # Test connections
#     test_conn_task = PythonOperator(
#         task_id='test_connections',
#         python_callable=test_connections,
#         provide_context=True,
#     )

#     # Download SEC data
#     download_task = PythonOperator(
#         task_id='download_sec_data',
#         python_callable=download_sec_data,
#         op_kwargs={'year': '2023', 'quarter': '4'},
#         provide_context=True,
#     )

#     # Upload to S3
#     upload_task = PythonOperator(
#     task_id='upload_to_s3',
#     python_callable=upload_to_s3,
#     op_kwargs={
#         'downloaded_files': "{{ task_instance.xcom_pull(task_ids='download_sec_data') }}",
#         'year': '2023', 
#         'quarter': '4',
#         'aws_access_key': aws_access_key,
#         'aws_secret_key': aws_secret_key
#     },
#     provide_context=True,
#     )

#     # Load to Snowflake
#     load_task = PythonOperator(
#         task_id='process_and_load_to_snowflake',
#         python_callable=process_and_load_to_snowflake,
#         op_kwargs={
#             'database': 'ASSIGNMENT2_TEAM1',
#             'schema': 'RAW_STAGING'
#         },
#         dag=dag
#     )

#     # Trigger DBT pipeline
#     trigger_dbt = TriggerDagRunOperator(
#         task_id='trigger_dbt_pipeline',
#         trigger_dag_id='dbt_transformation_pipeline',
#     )

#     # Set task dependencies
#     test_conn_task >> download_task >> upload_task >> load_task >> trigger_dbt

# print("Imports successful...")

# # Make sure this is at the end of your file
# if __name__ == "__main__":
#     dag.test()

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from test_connections import test_connections
from raw import download_sec_data, upload_to_s3, process_and_load_to_snowflake
from airflow.hooks.base import BaseHook

aws_conn = BaseHook.get_connection("aws_default")
aws_access_key = aws_conn.login
aws_secret_key = aws_conn.password

q= '4'
year='2023'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sec_data_pipeline',
    default_args=default_args,
    description='Download SEC financial statement data and upload to S3',
    schedule_interval=None,
    catchup=False
) as dag:

    test_conn_task = PythonOperator(
        task_id='test_connections',
        python_callable=test_connections,
    )

    download_task = PythonOperator(
        task_id='download_sec_data',
        python_callable=download_sec_data,
        op_kwargs={'year': year, 'quarter': q},
    )

    aws_conn = BaseHook.get_connection("aws_default")

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'downloaded_files': "{{ task_instance.xcom_pull(task_ids='download_sec_data') }}",
            'year': year,
            'quarter': q,
            'aws_access_key': aws_conn.login,
            'aws_secret_key': aws_conn.password,
        },
    )

    load_task = PythonOperator(
        task_id='process_and_load_to_snowflake',
        python_callable=process_and_load_to_snowflake,
        op_kwargs={
            'database': 'SEC_DATA',
            'schema': 'RAW_STAGING',
            'quarter': q,
            'year': year
        },
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_pipeline',
        trigger_dag_id='dbt_transformation_pipeline',
    )

test_conn_task >> download_task >> upload_task >> load_task >> trigger_dbt
