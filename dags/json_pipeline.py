
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.utils.dates import datetime
# import pandas as pd
# import io
# import json

# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }

# # Define the DAG
# with DAG(
#     dag_id='sec_txt_to_json_snowflake',
#     default_args=default_args,
#     description='Process SEC TXT files to JSON and load into Snowflake',
#     schedule_interval=None,
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     params={
#         "year": 2023,
#         "quarter": 4,
#     },
# ) as dag:

#     # Dummy start task
#     start_task = PythonOperator(
#         task_id='start_task',
#         python_callable=lambda: print("Starting SEC TXT to JSON pipeline."),
#     )

#     # Check if JSON files exist in S3
#     def check_s3_files(**kwargs):
#         year = kwargs['params']['year']
#         quarter = kwargs['params']['quarter']
#         bucket_name = "damg7245-assignment2-team-1"
#         s3_hook = S3Hook(aws_conn_id='aws_default')

#         # Define output path for JSON files
#         output_path = f"sec_data/{year}/q{quarter}/json/"
#         json_files = ["num.json", "pre.json", "sub.json", "tag.json"]

#         # Check if all required JSON files exist in S3
#         files_exist = all(
#             s3_hook.check_for_key(key=f"{output_path}{file_name}", bucket_name=bucket_name)
#             for file_name in json_files
#         )
        
#         # Push result to XCom
#         kwargs['ti'].xcom_push(key='files_exist', value=files_exist)

#     check_s3_files_task = PythonOperator(
#         task_id='check_s3_files',
#         python_callable=check_s3_files,
#         provide_context=True,
#     )

#     # Decide whether to process files or skip processing based on their existence in S3
#     def decide_on_processing(**kwargs):
#         # Pull the 'files_exist' flag from XCom
#         files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
        
#         # Decide which branch to follow
#         return 'skip_processing' if files_exist else 'process_txt_to_json'

#     decide_task = BranchPythonOperator(
#         task_id='decide_on_processing',
#         python_callable=decide_on_processing,
#         provide_context=True,
#     )

#     # Task to skip processing if JSON files already exist
#     def skip_processing():
#         print("JSON files already exist in S3. Skipping processing.")

#     skip_processing_task = PythonOperator(
#         task_id='skip_processing',
#         python_callable=skip_processing,
#     )

#     # Task to process TXT files and upload JSON to S3
#     def process_txt_to_json(**kwargs):
#         year = kwargs['params']['year']
#         quarter = kwargs['params']['quarter']
#         print(f"Year: {year}, Quarter: {quarter}")

#         bucket_name = 'damg7245-assignment2-team-1'
#         s3_hook = S3Hook(aws_conn_id='aws_default')

#         # Define input/output paths
#         input_path = f"sec_data/{year}/q{quarter}/raw/"
#         output_path = f"sec_data/{year}/q{quarter}/json/"
#         txt_files = ["num.txt", "pre.txt", "sub.txt", "tag.txt"]

#         for file_name in txt_files:
#             try:
#                 # Construct S3 key dynamically
#                 s3_key = f"{input_path}{file_name}"

#                 print(f"Checking S3 Key: {s3_key}, Bucket: {bucket_name}")
#                 file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
#                 content = file_obj.get()["Body"].read().decode("utf-8")

#                 # Process TXT content into structured data (example with pandas)
#                 df = pd.read_csv(io.StringIO(content), sep="\t")  # Adjust separator if needed

#                 # Convert DataFrame to JSON
#                 json_data = df.to_dict(orient="records")

#                 # Upload JSON file to S3
#                 json_file_name = file_name.replace(".txt", ".json")
#                 json_key = f"{output_path}{json_file_name}"
#                 s3_hook.load_string(json.dumps(json_data), key=json_key, bucket_name=bucket_name)
#                 print(f"Uploaded {json_key} to S3.")
#             except Exception as e:
#                 print(f"Error processing {file_name}: {e}")
#                 continue  # Skip this file and move on

#     process_txt_task = PythonOperator(
#         task_id='process_txt_to_json',
#         python_callable=process_txt_to_json,
#         provide_context=True,
#     )

#     # Snowflake tasks (unchanged but with TriggerRule set to ALL_DONE)
#     create_stage_task = SnowflakeOperator(
#         task_id='create_snowflake_stage',
#         sql="""
#             CREATE OR REPLACE STAGE sec_json_stage 
#             URL='s3://damg7245-assignment2-team-1/sec_data/{{ params.year }}/q{{ params.quarter }}/json/'
#             STORAGE_INTEGRATION=S3_INTEGRATION;
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule="all_done",  # Execute even if upstream fails
#     )

#     copy_into_table_task = SnowflakeOperator(
#         task_id='copy_into_snowflake_table',
#         sql="""
#             COPY INTO SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }}
#             FROM @sec_json_stage FILE_FORMAT=(TYPE=JSON);
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule="all_done",  # Execute even if upstream fails
#     )

#     create_views_task = SnowflakeOperator(
#         task_id='create_snowflake_views',
#         sql="""
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_BS_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
            
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_CF_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
            
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_IS_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
            
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_FV_{{ params.year }}_Q{{ params.quarter }} AS (
#                 SELECT *, 'Balance Sheet' AS section_type FROM JSON_BS_{{ params.year }}_Q{{ params.quarter }}
#                 UNION ALL
#                 SELECT *, 'Cash Flow' AS section_type FROM JSON_CF_{{ params.year }}_Q{{ params.quarter }}
#                 UNION ALL
#                 SELECT *, 'Income Statement' AS section_type FROM JSON_IS_{{ params.year }}_Q{{ params.quarter }}
#             );
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule="all_done",  # Execute even if upstream fails
#     )

#     end_task = PythonOperator(
#         task_id='end_task',
#         python_callable=lambda: print("Pipeline completed."),
#     )

#     # Define task dependencies
#     start_task >> check_s3_files_task >> decide_task >> [process_txt_task, skip_processing_task]
    
#     process_txt_task >> create_stage_task >> copy_into_table_task >> create_views_task >> end_task



# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.utils.dates import datetime
# import pandas as pd
# import io
# import json

# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }

# # Define the DAG
# with DAG(
#     dag_id='sec_txt_to_json_snowflake',
#     default_args=default_args,
#     description='Process SEC TXT files to JSON and load into Snowflake',
#     schedule_interval=None,
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     params={
#         "year": 2023,
#         "quarter": 4,
#     },
# ) as dag:

#     # Dummy start task
#     start_task = PythonOperator(
#         task_id='start_task',
#         python_callable=lambda: print("Starting SEC TXT to JSON pipeline."),
#     )

#     # Check if JSON files exist in S3
#     def check_s3_files(**kwargs):
#         year = kwargs['params']['year']
#         quarter = kwargs['params']['quarter']
#         bucket_name = "damg7245-assignment2-team-1"
#         s3_hook = S3Hook(aws_conn_id='aws_default')

#         # Define output path for JSON files
#         output_path = f"sec_data/{year}/q{quarter}/json/"
#         json_files = ["num.json", "pre.json", "sub.json", "tag.json"]

#         # Check if all required JSON files exist in S3
#         files_exist = all(
#             s3_hook.check_for_key(key=f"{output_path}{file_name}", bucket_name=bucket_name)
#             for file_name in json_files
#         )
        
#         # Push result to XCom
#         kwargs['ti'].xcom_push(key='files_exist', value=files_exist)

#     check_s3_files_task = PythonOperator(
#         task_id='check_s3_files',
#         python_callable=check_s3_files,
#         provide_context=True,
#     )

#     # Decide whether to process files or skip processing based on their existence in S3
#     def decide_on_processing(**kwargs):
#         # Pull the 'files_exist' flag from XCom
#         files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
        
#         # Decide which branch to follow
#         return 'skip_processing' if files_exist else 'process_txt_to_json'

#     decide_task = BranchPythonOperator(
#         task_id='decide_on_processing',
#         python_callable=decide_on_processing,
#         provide_context=True,
#     )

#     # Task to skip processing if JSON files already exist
#     def skip_processing():
#         print("JSON files already exist in S3. Skipping processing.")

#     skip_processing_task = PythonOperator(
#         task_id='skip_processing',
#         python_callable=skip_processing,
#     )

#     # Task to process TXT files and upload JSON to S3
#     def process_txt_to_json(**kwargs):
#         year = kwargs['params']['year']
#         quarter = kwargs['params']['quarter']
#         print(f"Year: {year}, Quarter: {quarter}")

#         bucket_name = 'damg7245-assignment2-team-1'
#         s3_hook = S3Hook(aws_conn_id='aws_default')

#         # Define input/output paths
#         input_path = f"sec_data/{year}/q{quarter}/raw/"
#         output_path = f"sec_data/{year}/q{quarter}/json/"
#         txt_files = ["num.txt", "pre.txt", "sub.txt", "tag.txt"]

#         for file_name in txt_files:
#             try:
#                 # Construct S3 key dynamically
#                 s3_key = f"{input_path}{file_name}"

#                 print(f"Checking S3 Key: {s3_key}, Bucket: {bucket_name}")
#                 file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
#                 content = file_obj.get()["Body"].read().decode("utf-8")

#                 # Process TXT content into structured data (example with pandas)
#                 df = pd.read_csv(io.StringIO(content), sep="\t")  # Adjust separator if needed

#                 # Convert DataFrame to JSON
#                 json_data = df.to_dict(orient="records")

#                 # Upload JSON file to S3
#                 json_file_name = file_name.replace(".txt", ".json")
#                 json_key = f"{output_path}{json_file_name}"
#                 s3_hook.load_string(json.dumps(json_data), key=json_key, bucket_name=bucket_name)
#                 print(f"Uploaded {json_key} to S3.")
#             except Exception as e:
#                 print(f"Error processing {file_name}: {e}")
#                 continue  # Skip this file and move on

#     process_txt_task = PythonOperator(
#         task_id='process_txt_to_json',
#         python_callable=process_txt_to_json,
#         provide_context=True,
#     )

#     # Task to create Snowflake table dynamically based on year and quarter variables
#     create_table_task_sql_template = """
#             CREATE TABLE IF NOT EXISTS SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }} (
#             data VARIANT);
#             """
    
#     create_table_task = SnowflakeOperator(
#         task_id='create_snowflake_table',
#         sql=create_table_task_sql_template,
#         snowflake_conn_id='snowflake_default',
#     )

#     create_stage_task = SnowflakeOperator(
#         task_id='create_snowflake_stage',
#         sql="""
#             CREATE OR REPLACE STAGE sec_json_stage 
#             URL='s3://damg7245-assignment2-team-1/sec_data/{{ params.year }}/q{{ params.quarter }}/json/'
#             STORAGE_INTEGRATION=S3_INTEGRATION;
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule="all_done",  # Execute even if upstream fails
#     )

#     copy_into_table_task = SnowflakeOperator(
#         task_id='copy_into_snowflake_table',
#         sql="""
#             COPY INTO SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }}
#             FROM @sec_json_stage FILE_FORMAT=(TYPE=JSON);
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule="all_done",  # Execute even if upstream fails
#     )

#     create_views_task = SnowflakeOperator(
#         task_id='create_snowflake_views',
#         sql="""
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_BS_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
            
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_CF_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
            
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_IS_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
            
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_FV_{{ params.year }}_Q{{ params.quarter }} AS (
#                 SELECT *, 'Balance Sheet' AS section_type FROM JSON_BS_{{ params.year }}_Q{{ params.quarter }}
#                 UNION ALL
#                 SELECT *, 'Cash Flow' AS section_type FROM JSON_CF_{{ params.year }}_Q{{ params.quarter }}
#                 UNION ALL
#                 SELECT *, 'Income Statement' AS section_type FROM JSON_IS_{{ params.year }}_Q{{ params.quarter }}
#             );
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule="all_done",  # Execute even if upstream fails
#     )

#     end_task = PythonOperator(
#         task_id='end_task',
#         python_callable=lambda: print("Pipeline completed."),
#     )

#     # Define task dependencies
#     start_task >> check_s3_files_task >> decide_task >> [process_txt_task, skip_processing_task]
    
#     process_txt_task >> create_table_task >> create_stage_task >> copy_into_table_task >> create_views_task >> end_task


















# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.utils.dates import datetime
# from airflow.utils.trigger_rule import TriggerRule  # Import TriggerRule
# import pandas as pd
# import io
# import json
# import gzip

# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }

# # Define the DAG
# with DAG(
#     dag_id='sec_txt_to_json_snowflake',
#     default_args=default_args,
#     description='Process SEC TXT files to JSON and load into Snowflake',
#     schedule_interval=None,
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     params={
#         "year": 2023,
#         "quarter": 4,
#     },
# ) as dag:

#     # Dummy start task
#     start_task = PythonOperator(
#         task_id='start_task',
#         python_callable=lambda: print("Starting SEC TXT to JSON pipeline."),
#     )

#     # Check if JSON files exist in S3
#     def check_s3_files(**kwargs):
#         year = kwargs['params']['year']
#         quarter = kwargs['params']['quarter']
#         bucket_name = "damg7245-assignment2-team-1"
#         s3_hook = S3Hook(aws_conn_id='aws_default')
#         output_path = f"sec_data/{year}/q{quarter}/json/"
#         json_files = ["num.json", "pre.json", "sub.json", "tag.json"]
#         files_exist = all(
#             s3_hook.check_for_key(key=f"{output_path}{file_name}", bucket_name=bucket_name)
#             for file_name in json_files
#         )
#         kwargs['ti'].xcom_push(key='files_exist', value=files_exist)

#     check_s3_files_task = PythonOperator(
#         task_id='check_s3_files',
#         python_callable=check_s3_files,
#         provide_context=True,
#     )

#     # Decide whether to process files or skip processing based on their existence in S3
#     def decide_on_processing(**kwargs):
#         files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
#         return 'skip_processing' if files_exist else 'process_txt_to_json'

#     decide_task = BranchPythonOperator(
#         task_id='decide_on_processing',
#         python_callable=decide_on_processing,
#         provide_context=True,
#     )

#     # Task to skip processing if JSON files already exist
#     def skip_processing():
#         print("JSON files already exist in S3. Skipping processing.")

#     skip_processing_task = PythonOperator(
#         task_id='skip_processing',
#         python_callable=skip_processing,
#     )

#     # Task to process TXT files and upload JSON to S3
#     def process_txt_to_json(**kwargs):
#         year = kwargs['params']['year']
#         quarter = kwargs['params']['quarter']
#         bucket_name = 'damg7245-assignment2-team-1'
#         s3_hook = S3Hook(aws_conn_id='aws_default')
#         input_path = f"sec_data/{year}/q{quarter}/raw/"
#         output_path = f"sec_data/{year}/q{quarter}/json/"
#         txt_files = ["num.txt", "pre.txt", "sub.txt", "tag.txt"]

#         for file_name in txt_files:
#             try:
#                 s3_key = f"{input_path}{file_name}"
#                 file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
#                 content = file_obj.get()["Body"].read().decode("utf-8")
#                 df = pd.read_csv(io.StringIO(content), sep="\t")

#                 # Split DataFrame into smaller chunks to avoid large JSON objects
#                 chunk_size = 500  # Adjust based on data size
#                 for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
#                     chunk_df = df.iloc[chunk_start:chunk_start + chunk_size]
#                     json_data = chunk_df.to_dict(orient="records")

#                     # Convert JSON data to string
#                     json_string = json.dumps(json_data)

#                     # Compress JSON using gzip
#                     compressed_data = gzip.compress(json_string.encode('utf-8'))

#                     # Upload compressed JSON file to S3
#                     json_file_name = file_name.replace(".txt", f"_{i}.json.gz")
#                     json_key = f"{output_path}{json_file_name}"

#                     s3_hook.load_bytes(compressed_data, key=json_key, bucket_name=bucket_name)
#                     print(f"Uploaded {json_key} to S3.")
#             except Exception as e:
#                 print(f"Error processing {file_name}: {e}")
#                 continue

#     process_txt_task = PythonOperator(
#         task_id='process_txt_to_json',
#         python_callable=process_txt_to_json,
#         provide_context=True,
#     )

#     # Task to check and create Snowflake table if it does not exist
#     check_and_create_table_sql_template = """
#         CREATE TABLE IF NOT EXISTS SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }} (
#             data VARIANT
#         );
#     """

#     check_and_create_table_task = SnowflakeOperator(
#         task_id='check_and_create_table',
#         sql=check_and_create_table_sql_template,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule=TriggerRule.ALL_DONE,  # Ensure this runs even if upstream is skipped
#     )

#     create_stage_task = SnowflakeOperator(
#         task_id='create_snowflake_stage',
#         sql="""
#             CREATE OR REPLACE STAGE sec_json_stage 
#             URL='s3://damg7245-assignment2-team-1/sec_data/{{ params.year }}/q{{ params.quarter }}/json/'
#             STORAGE_INTEGRATION=S3_INTEGRATION;
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule=TriggerRule.ALL_DONE,  # Ensure this runs even if upstream is skipped
#     )

#     copy_into_table_task = SnowflakeOperator(
#         task_id='copy_into_snowflake_table',
#         sql="""
#             COPY INTO SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }}
#             FROM @sec_json_stage FILE_FORMAT=(TYPE=JSON);
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule=TriggerRule.ALL_DONE,  # Ensure this runs even if upstream is skipped
#     )

#     create_views_task = SnowflakeOperator(
#         task_id='create_snowflake_views',
#         sql="""
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_BS_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_CF_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_IS_{{ params.year }}_Q{{ params.quarter }} AS SELECT ...;
#             CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_FV_{{ params.year }}_Q{{ params.quarter }} AS (
#                 SELECT *, 'Balance Sheet' AS section_type FROM JSON_BS_{{ params.year }}_Q{{ params.quarter }}
#                 UNION ALL
#                 SELECT *, 'Cash Flow' AS section_type FROM JSON_CF_{{ params.year }}_Q{{ params.quarter }}
#                 UNION ALL
#                 SELECT *, 'Income Statement' AS section_type FROM JSON_IS_{{ params.year }}_Q{{ params.quarter }}
#             );
#         """,
#         snowflake_conn_id='snowflake_default',
#         trigger_rule=TriggerRule.ALL_DONE,  # Ensure this runs even if upstream is skipped
#     )

#     end_task = PythonOperator(
#         task_id='end_task',
#         python_callable=lambda: print("Pipeline completed."),
#     )

#     # Define task dependencies
#     start_task >> check_s3_files_task >> decide_task >> [process_txt_task, skip_processing_task]
    
#     [process_txt_task, skip_processing_task] >> check_and_create_table_task >> create_stage_task >> copy_into_table_task >> create_views_task >> end_task


from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import datetime
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import io
import json
import gzip

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='sec_txt_to_json_snowflake',
    default_args=default_args,
    description='Process SEC TXT files to JSON and load into Snowflake',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "year": 2023,
        "quarter": 4,
    },
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: print("Starting SEC TXT to JSON pipeline."),
    )

    def check_s3_files(**kwargs):
        year = kwargs['params']['year']
        quarter = kwargs['params']['quarter']
        bucket_name = "damg7245-assignment2-team-1"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        output_path = f"sec_data/{year}/q{quarter}/json/"

        # List all files in S3 json directory
        existing_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=output_path)

        files_exist = bool(existing_files)  # True if JSON files exist
        kwargs['ti'].xcom_push(key='files_exist', value=files_exist)

    check_s3_files_task = PythonOperator(
        task_id='check_s3_files',
        python_callable=check_s3_files,
    )

    def decide_on_processing(**kwargs):
        files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
        return 'skip_processing' if files_exist else 'process_txt_to_json'

    decide_task = BranchPythonOperator(
        task_id='decide_on_processing',
        python_callable=decide_on_processing,
    )

    def skip_processing():
        print("JSON files already exist in S3. Skipping processing.")

    skip_processing_task = PythonOperator(
        task_id='skip_processing',
        python_callable=skip_processing,
    )

    def process_txt_to_json(**kwargs):
        year = kwargs['params']['year']
        quarter = kwargs['params']['quarter']
        bucket_name = 'damg7245-assignment2-team-1'
        s3_hook = S3Hook(aws_conn_id='aws_default')
        input_path = f"sec_data/{year}/q{quarter}/raw/"
        output_path = f"sec_data/{year}/q{quarter}/json/"
        txt_files = ["num.txt", "pre.txt", "sub.txt", "tag.txt"]

        for file_name in txt_files:
            try:
                s3_key = f"{input_path}{file_name}"
                file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
                content = file_obj.get()["Body"].read().decode("utf-8")
                df = pd.read_csv(io.StringIO(content), sep="\t")

                # Split DataFrame into smaller chunks
                chunk_size = 200
                for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
                    chunk_df = df.iloc[chunk_start:chunk_start + chunk_size]
                    json_data = chunk_df.to_dict(orient="records")

                    compressed_data = gzip.compress(json.dumps(json_data).encode('utf-8'))

                    json_file_name = file_name.replace(".txt", f"_{i}.json.gz")
                    json_key = f"{output_path}{json_file_name}"

                    s3_hook.load_bytes(compressed_data, key=json_key, bucket_name=bucket_name)
                    print(f"Uploaded {json_key} to S3.")
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

    process_txt_task = PythonOperator(
        task_id='process_txt_to_json',
        python_callable=process_txt_to_json,
    )

    check_and_create_table_task = SnowflakeOperator(
        task_id='check_and_create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }} (
                data VARIANT
            );
        """,
        snowflake_conn_id='snowflake_default',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_stage_task = SnowflakeOperator(
        task_id='create_snowflake_stage',
        sql="""
            CREATE OR REPLACE STAGE sec_json_stage 
            URL='s3://damg7245-assignment2-team-1/sec_data/{{ params.year }}/q{{ params.quarter }}/json/'
            STORAGE_INTEGRATION=S3_INTEGRATION;
        """,
        snowflake_conn_id='snowflake_default',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    copy_into_table_task = SnowflakeOperator(
        task_id='copy_into_snowflake_table',
        sql="""
            COPY INTO SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }}
            FROM @sec_json_stage
            FILE_FORMAT=(TYPE=JSON, STRIP_OUTER_ARRAY=TRUE);
        """,
        snowflake_conn_id='snowflake_default',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_views_task = SnowflakeOperator(
        task_id='create_snowflake_views',
        sql="""
            CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_BS_{{ params.year }}_Q{{ params.quarter }} AS 
            SELECT data FROM SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }};

            CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_CF_{{ params.year }}_Q{{ params.quarter }} AS 
            SELECT data FROM SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }};

            CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_IS_{{ params.year }}_Q{{ params.quarter }} AS 
            SELECT data FROM SEC_DATA.JSON_STAGING.SEC_JSON_{{ params.year }}_Q{{ params.quarter }};

            CREATE OR REPLACE VIEW SEC_DATA.JSON_STAGING.JSON_FV_{{ params.year }}_Q{{ params.quarter }} AS 
            SELECT data, 'Balance Sheet' AS section_type FROM JSON_BS_{{ params.year }}_Q{{ params.quarter }}
            UNION ALL
            SELECT data, 'Cash Flow' AS section_type FROM JSON_CF_{{ params.year }}_Q{{ params.quarter }}
            UNION ALL
            SELECT data, 'Income Statement' AS section_type FROM JSON_IS_{{ params.year }}_Q{{ params.quarter }};
        """,
        snowflake_conn_id='snowflake_default',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: print("Pipeline completed."),
    )

    start_task >> check_s3_files_task >> decide_task >> [process_txt_task, skip_processing_task]
    [process_txt_task, skip_processing_task] >> check_and_create_table_task >> create_stage_task >> copy_into_table_task >> create_views_task >> end_task
