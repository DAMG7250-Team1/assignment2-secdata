# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta

# from load_airflow_variables import load_airflow_variables
# from web_scrapper import download_quarterly_data
# from zip_ext_and_parq_store import SECDataProcessor
# from s3_data_checker import is_data_present_in_s3
# import subprocess


# # **Step 5: Run DBT Pipeline**
# DBT_PROJECT_DIR = "/opt/airflow/financial_dbt_project"
# DBT_PROFILE_DIR = "/opt/airflow/financial_dbt_project/profiles"

# # **Step 2: Check Data in S3**
# def check_data_and_set_variable(**context):
#     """Checks if data is present in S3 and sets a variable"""
#     year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
#     quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    
#     if is_data_present_in_s3(year, quarter):
#         context['task_instance'].xcom_push(key='run_dbt', value=True)
#     else:
#         context['task_instance'].xcom_push(key='run_dbt', value=False)

# def scrape_sec_data(**context):
#     """Fetches SEC ZIP files and stores them in S3"""
#     year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
#     quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
#     download_quarterly_data(year, quarter)

# def extract_and_convert(**context):
#     """Extracts SEC ZIP files and converts to Parquet"""
#     year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
#     quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
#     processor = SECDataProcessor()
#     processor.extract_zip_file(year, quarter)

# def run_dbt_pipeline(**context):
#     """Runs the DBT pipeline using subprocess."""
#     year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
#     quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    
#     dbt_command = [
#         "dbt", "run",
#         "--project-dir", "/opt/airflow/financial_dbt_project",
#         "--profiles-dir", "/opt/airflow/financial_dbt_project/profiles",
#         "--vars", f"{{'year': {year}, 'quarter': {quarter}}}"
#     ]
    
#     subprocess.run(dbt_command, check=True)


# # Default DAG arguments
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'dft_data_pipeline',
#     default_args=default_args,
#     description='DFT Data Ingestion Pipeline',
#     schedule_interval=None,
#     catchup=False
# )

# task_load_variables = PythonOperator(
#     task_id='load_airflow_variables',
#     python_callable=load_airflow_variables,
#     provide_context=True,
#     dag=dag
# )


# task_check_data_and_set_variable = PythonOperator(
#     task_id='check_data_and_set_variable',
#     python_callable=check_data_and_set_variable,
#     provide_context=True,
#     dag=dag
# )

# task_decide_dbt = BranchPythonOperator(
#     task_id='decide_dbt',
#     python_callable=lambda **context: ['run_dbt_pipeline'] if context['task_instance'].xcom_pull(task_ids='check_data_and_set_variable', key='run_dbt') else ['scrape_sec_data'],
#     provide_context=True,
#     dag=dag
# )

# # **Step 3: Scrape SEC Data**
# task_scrape_sec = PythonOperator(
#     task_id='scrape_sec_data',
#     python_callable=scrape_sec_data,
#     provide_context=True,
#     dag=dag
# )

# # **Step 4: Extract & Convert Data**
# task_extract_convert = PythonOperator(
#     task_id='extract_and_convert',
#     python_callable=extract_and_convert,
#     provide_context=True,
#     dag=dag
# )


# task_run_dbt = BashOperator(
#     task_id='run_dbt_pipeline',
#     bash_command=(
#         "bash /opt/airflow/sec_pipeline/run_dbt_pipeline.sh "
#         "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_year') }} "
#         "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_quarter') }}"
#     ),
#     dag=dag
# )

# # Set Task Dependencies
# task_load_variables >> task_check_data_and_set_variable
# task_check_data_and_set_variable >> task_decide_dbt
# task_decide_dbt >> task_run_dbt
# task_decide_dbt >> task_scrape_sec
# task_scrape_sec >> task_extract_convert >> task_run_dbt


# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from datetime import datetime, timedelta
# from load_airflow_variables import load_airflow_variables
# from web_scrapper import download_quarterly_data
# from zip_ext_and_parq_store import SECDataProcessor
# import subprocess
# import os

# # Configuration
# DBT_PROJECT_DIR = "/opt/airflow/financial_dbt_project"
# AWS_CONN_ID = "aws_default"

# def check_data_and_set_variable(**context):
#     """Checks if data is present in S3 using Airflow's S3Hook"""
#     year = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
#     quarter = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    
#     hook = S3Hook(aws_conn_id=AWS_CONN_ID)
#     prefix = f"sec_data/{year}/q{quarter}/raw/"
    
#     exists = hook.check_for_prefix(bucket_name="damg7245-assignment2-team-1", prefix=prefix, delimiter='/')
#     context['ti'].xcom_push(key='run_dbt', value=exists)
#     return exists


# def run_dbt_pipeline(**context):
#     """Runs the DBT pipeline with proper environment variables"""
#     year = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
#     quarter = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    
#     dbt_command = [
#         "dbt", "run",
#         "--project-dir", DBT_PROJECT_DIR,
#         "--profiles-dir", DBT_PROJECT_DIR,
#         "--vars", f"{{'year': {year}, 'quarter': {quarter}}}"
#     ]
    
#     subprocess.run(dbt_command, check=True, env={
#         **os.environ,
#         "DBT_SNOWFLAKE_ACCOUNT": "{{ conn.snowflake.extra_dejson.account }}",
#         "DBT_SNOWFLAKE_USER": "{{ conn.snowflake.login }}",
#         "DBT_SNOWFLAKE_PASSWORD": "{{ conn.snowflake.password }}",
#         "DBT_SNOWFLAKE_ROLE": "{{ conn.snowflake.extra_dejson.role }}",
#         "DBT_SNOWFLAKE_DATABASE": "{{ conn.snowflake.extra_dejson.database }}",
#         "DBT_SNOWFLAKE_WAREHOUSE": "{{ conn.snowflake.extra_dejson.warehouse }}"
#     })

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'dft_data_pipeline',
#     default_args=default_args,
#     description='DFT Data Ingestion Pipeline',
#     schedule_interval=None,
#     catchup=False
# )

# task_load_variables = PythonOperator(
#     task_id='load_airflow_variables',
#     python_callable=load_airflow_variables,
#     provide_context=True,
#     dag=dag
# )

# task_check_data = PythonOperator(
#     task_id='check_s3_data',
#     python_callable=check_data_and_set_variable,
#     provide_context=True,
#     dag=dag
# )

# task_decide_branch = BranchPythonOperator(
#     task_id='decide_pipeline_path',
#     python_callable=lambda **ctx: 'run_dbt_pipeline' if ctx['ti'].xcom_pull(task_ids='check_s3_data', key='run_dbt') else 'scrape_data',
#     provide_context=True,
#     dag=dag
# )

# task_scrape = PythonOperator(
#     task_id='scrape_data',
#     python_callable=download_quarterly_data,
#     op_kwargs={'year': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_year') }}",
#                'quarter': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_quarter') }}"},
#     dag=dag
# )

# task_process_data = PythonOperator(
#     task_id='process_data',
#     python_callable=SECDataProcessor().extract_zip_file,
#     op_kwargs={'year': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_year') }}",
#                'quarter': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_quarter') }}"},
#     dag=dag
# )

# task_run_dbt = PythonOperator(
#     task_id='run_dbt_pipeline',
#     python_callable=run_dbt_pipeline,
#     provide_context=True,
#     dag=dag
# )

# # Dependency setup
# task_load_variables >> task_check_data >> task_decide_branch
# task_decide_branch >> [task_run_dbt, task_scrape]
# task_scrape >> task_process_data >> task_run_dbt






from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from load_airflow_variables import load_airflow_variables
from web_scrapper import download_quarterly_data
from zip_ext_and_parq_store import SECDataProcessor
import subprocess
import os

# Configuration
DBT_PROJECT_DIR = "/opt/airflow/financial_dbt_project"
AWS_CONN_ID = "aws_default"

def check_data_and_set_variable(**context):
    """Checks if data is present in S3 using Airflow's S3Hook"""
    year = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    prefix = f"sec_data/{year}/q{quarter}/raw/"
    exists = hook.check_for_prefix(bucket_name="damg7245-assignment2-team-1", prefix=prefix, delimiter='/')
    context['ti'].xcom_push(key='run_dbt', value=exists)
    return exists

def run_dbt_pipeline(**context):
    """Runs the DBT pipeline with proper environment variables"""
    year = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    dbt_command = [
        "dbt", "run",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
        "--vars", f"{{'year': {year}, 'quarter': {quarter}}}"
    ]
    subprocess.run(dbt_command, check=True, env={
        **os.environ,
        "DBT_SNOWFLAKE_ACCOUNT": "{{ conn.snowflake.extra_dejson.account }}",
        "DBT_SNOWFLAKE_USER": "{{ conn.snowflake.login }}",
        "DBT_SNOWFLAKE_PASSWORD": "{{ conn.snowflake.password }}",
        "DBT_SNOWFLAKE_ROLE": "{{ conn.snowflake.extra_dejson.role }}",
        "DBT_SNOWFLAKE_DATABASE": "{{ conn.snowflake.extra_dejson.database }}",
        "DBT_SNOWFLAKE_WAREHOUSE": "{{ conn.snowflake.extra_dejson.warehouse }}"
    })

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dft_data_pipeline',
    default_args=default_args,
    description='DFT Data Ingestion Pipeline',
    schedule_interval=None,
    catchup=False
)

task_load_variables = PythonOperator(
    task_id='load_airflow_variables',
    python_callable=load_airflow_variables,
    provide_context=True,
    dag=dag
)

task_check_data = PythonOperator(
    task_id='check_s3_data',
    python_callable=check_data_and_set_variable,
    provide_context=True,
    dag=dag
)

# task_decide_branch = BranchPythonOperator(
#     task_id='decide_pipeline_path',
#     python_callable=lambda **ctx: ['run_dbt_pipeline'] if ctx['ti'].xcom_pull(task_ids='check_s3_data', key='run_dbt') else ['scrape_data', 'process_data'],
#     provide_context=True,
#     dag=dag
# )

# task_scrape = PythonOperator(
#     task_id='scrape_data',
#     python_callable=download_quarterly_data,
#     op_kwargs={'year': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_year') }}",
#                'quarter': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_quarter') }}"},
#     dag=dag
# )

task_process_data = PythonOperator(
    task_id='process_data',
    python_callable=SECDataProcessor().extract_zip_file,
    op_kwargs={'year': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_year') }}",
               'quarter': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_quarter') }}"},
    dag=dag
)

task_run_dbt = PythonOperator(
    task_id='run_dbt_pipeline',
    python_callable=run_dbt_pipeline,
    provide_context=True,
    dag=dag
)

# Dependency setup
# task_load_variables >> task_check_data >> task_decide_branch
# task_decide_branch >> [task_run_dbt, task_scrape]
# task_scrape >> task_process_data
# task_process_data >> task_run_dbt


task_load_variables >> task_check_data >> task_process_data
task_process_data >> task_run_dbt



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from load_airflow_variables import load_airflow_variables
from web_scrapper import download_quarterly_data
from zip_ext_and_parq_store import SECDataProcessor
import subprocess
import os

# Configuration
DBT_PROJECT_DIR = "/opt/airflow/financial_dbt_project"
AWS_CONN_ID = "aws_default"

def check_data_and_set_variable(**context):
    year = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    prefix = f"sec_data/{year}/q{quarter}/raw/"
    required_files = ['num.txt', 'sub.txt', 'tag.txt', 'pre.txt']
    
    all_files_exist = all(hook.check_for_key(f"{prefix}{file}", bucket_name="damg7245-assignment2-team-1") for file in required_files)
    context['ti'].xcom_push(key='run_dbt', value=all_files_exist)
    return all_files_exist


def conditional_scrape(**context):
    """Conditionally scrapes data if not present in S3"""
    if not context['ti'].xcom_pull(task_ids='check_s3_data', key='run_dbt'):
        year = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
        quarter = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
        download_quarterly_data(year, quarter)

def run_dbt_pipeline(**context):
    """Runs the DBT pipeline with proper environment variables"""
    year = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['ti'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    dbt_command = [
        "dbt", "run",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
        "--vars", f"{{'year': {year}, 'quarter': {quarter}}}"
    ]
    subprocess.run(dbt_command, check=True, env={
        **os.environ,
        "DBT_SNOWFLAKE_ACCOUNT": "{{ conn.snowflake.extra_dejson.account }}",
        "DBT_SNOWFLAKE_USER": "{{ conn.snowflake.login }}",
        "DBT_SNOWFLAKE_PASSWORD": "{{ conn.snowflake.password }}",
        "DBT_SNOWFLAKE_ROLE": "{{ conn.snowflake.extra_dejson.role }}",
        "DBT_SNOWFLAKE_DATABASE": "{{ conn.snowflake.extra_dejson.database }}",
        "DBT_SNOWFLAKE_WAREHOUSE": "{{ conn.snowflake.extra_dejson.warehouse }}"
    })

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dft_data_pipeline',
    default_args=default_args,
    description='DFT Data Ingestion Pipeline',
    schedule_interval=None,
    catchup=False
)

task_load_variables = PythonOperator(
    task_id='load_airflow_variables',
    python_callable=load_airflow_variables,
    provide_context=True,
    dag=dag
)

task_check_data = PythonOperator(
    task_id='check_s3_data',
    python_callable=check_data_and_set_variable,
    provide_context=True,
    dag=dag
)

task_conditional_scrape = PythonOperator(
    task_id='conditional_scrape',
    python_callable=conditional_scrape,
    provide_context=True,
    dag=dag
)

task_process_data = PythonOperator(
    task_id='process_data',
    python_callable=SECDataProcessor().extract_zip_file,
    op_kwargs={'year': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_year') }}",
               'quarter': "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_quarter') }}"},
    dag=dag
)

task_run_dbt = PythonOperator(
    task_id='run_dbt_pipeline',
    python_callable=run_dbt_pipeline,
    provide_context=True,
    dag=dag
)

# Dependency setup
task_load_variables >> task_check_data >> task_conditional_scrape >> task_process_data >> task_run_dbt


