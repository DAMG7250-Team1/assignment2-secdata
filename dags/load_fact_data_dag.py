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
#         "--project-dir", "/opt/airflow/sec_pipeline",
#         "--profiles-dir", "/opt/airflow/sec_pipeline/profiles",
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
#         "dbt run "
#         "--project-dir /opt/airflow/financial_dbt_project "
#         "--profiles-dir /opt/airflow/financial_dbt_project/profiles "
#         "--vars '{{\"year\": {{ ti.xcom_pull(task_ids=\"load_airflow_variables\", key=\"sec_year\") }}, \"quarter\": {{ ti.xcom_pull(task_ids=\"load_airflow_variables\", key=\"sec_quarter\") }}}}'"
#     ),
#     dag=dag,
# )


# # Set Task Dependencies
# task_load_variables >> task_check_data_and_set_variable
# task_check_data_and_set_variable >> task_decide_dbt
# task_decide_dbt >> task_run_dbt
# task_decide_dbt >> task_scrape_sec
# task_scrape_sec >> task_extract_convert >> task_run_dbt



from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define constants
DBT_PROJECT_DIR = "/opt/airflow/dags/financial_dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dags/financial_dbt_project"

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_transformation_pipeline',
    default_args=default_args,
    description='DBT transformations for SEC financial data',
    schedule_interval=None,
    catchup=False
)

# DBT tasks
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f'pip3 install --quiet dbt-snowflake==1.5.0 && export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt run --models staging.* --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_run_facts = BashOperator(
    task_id='dbt_run_facts',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt run --models +fact_balance_sheet fact_cashflow fact_income_statement --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

# Set task dependencies
dbt_deps >> dbt_run_staging >> dbt_run_facts >> dbt_test >> dbt_docs