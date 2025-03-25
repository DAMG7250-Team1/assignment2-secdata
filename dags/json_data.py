# import json
# import boto3
# from airflow.hooks.base import BaseHook
# from snowflake.connector import connect

# def read_json_from_s3():
#     """Read JSON data from S3"""
#     try:
#         aws_conn = BaseHook.get_connection('aws_default')
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_conn.login,
#             aws_secret_access_key=aws_conn.password,
#             region_name='us-east-1'
#         )

#         bucket_name = 'damg7245-assignment2-team-1'
#         file_key = ''

#         print(f"Reading JSON from S3: {bucket_name}/{file_key}")

#         response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
#         json_data = json.loads(response['Body'].read().decode('utf-8'))

#         return json_data

#     except Exception as e:
#         print(f"Error reading JSON from S3: {str(e)}")
#         raise

# def load_json_to_snowflake(json_data):
#     """Load JSON data to Snowflake"""
#     try:
#         snowflake_conn = BaseHook.get_connection('snowflake_default')
        
#         conn = connect(
#             user=snowflake_conn.login,
#             password=snowflake_conn.password,
#             account=snowflake_conn.extra_dejson.get('account'),
#             warehouse=snowflake_conn.extra_dejson.get('warehouse'),
#             database=snowflake_conn.schema,  # Adjust accordingly if needed
#             schema='raw_staging'
#         )

#         cursor = conn.cursor()

#         print("Connected to Snowflake")
        
#         # Example logic to insert JSON data into Snowflake table (adjust accordingly)
#         insert_sql = """
#             INSERT INTO raw_json_table (json_column)
#             SELECT PARSE_JSON(%s)
#         """
        
#         cursor.execute(insert_sql, (json.dumps(json_data),))
        
#         cursor.close()
#         conn.close()

#         print("JSON data loaded successfully")

#     except Exception as e:
#         print(f"Error loading JSON to Snowflake: {str(e)}")
#         raise

# def verify_data_load():
#     """Verify data was loaded correctly"""
#     try:
#         snowflake_conn = BaseHook.get_connection('snowflake_default')

#         conn = connect(
#             user=snowflake_conn.login,
#             password=snowflake_conn.password,
#             account=snowflake_conn.extra_dejson.get('account'),
#             warehouse=snowflake_conn.extra_dejson.get('warehouse'),
#             database=snowflake_conn.schema,  # Adjust accordingly if needed
#             schema='raw_staging'
#         )

#         cursor = conn.cursor()

#         print("Verifying data load")

#         cursor.execute("SELECT COUNT(*) FROM raw_json_table")
        
#         result = cursor.fetchone()
        
#         print(f"Total records loaded: {result[0]}")

#         cursor.close()
#         conn.close()

#     except Exception as e:
#         print(f"Error verifying data load: {str(e)}")
#         raise


import json
import boto3
import pandas as pd
from airflow.hooks.base import BaseHook
from snowflake.connector import connect
import os

def process_raw_files_to_json(bucket_name, year, quarter):
    """Process raw SEC files from S3, convert them to JSON, and upload directly to S3."""
    try:
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )

        raw_files = ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']
        temp_dir = "/tmp/sec_data"
        os.makedirs(temp_dir, exist_ok=True)

        json_data = {}

        for file in raw_files:
            s3_key = f"sec_data/{year}q{quarter}/raw/{file}"
            local_file_path = os.path.join(temp_dir, file)
            s3_client.download_file(bucket_name, s3_key, local_file_path)

            df = pd.read_csv(local_file_path, sep='\t', encoding='utf-8')
            json_data[file.split('.')[0]] = df.to_dict(orient='records')

            os.remove(local_file_path)

        json_file_path = os.path.join(temp_dir, f"{year}q{quarter}_data.json")
        with open(json_file_path, 'w') as json_file:
            json.dump(json_data, json_file)

        # Upload directly to S3 here
        processed_s3_key = f"sec_data/{year}q{quarter}/processed/data.json"
        s3_client.upload_file(json_file_path, bucket_name, processed_s3_key)
        print(f"JSON data uploaded to S3 at: {bucket_name}/{processed_s3_key}")

        # Return the S3 key instead of local path
        return processed_s3_key

    except Exception as e:
        print(f"Error processing and uploading JSON: {str(e)}")
        raise


def upload_json_to_s3(json_file_path, bucket_name, year, quarter):
    """Upload the generated JSON file to S3."""
    try:
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )

        s3_key = f"sec_data/{year}q{quarter}/processed/data.json"
        s3_client.upload_file(json_file_path, bucket_name, s3_key)
        print(f"JSON file uploaded to S3: {bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Error uploading JSON to S3: {str(e)}")
        raise


# def load_json_to_snowflake(bucket_name, year, quarter):
#     """Load JSON data from S3 into Snowflake."""
#     try:
#         snowflake_conn = BaseHook.get_connection('snowflake_default')

#         conn = connect(
#             user=snowflake_conn.login,
#             password=snowflake_conn.password,
#             account=snowflake_conn.extra_dejson.get('account'),
#             warehouse=snowflake_conn.extra_dejson.get('warehouse'),
#             database='SEC_DATA',  
#             schema='JSON_STAGING' 
#         )

#         cursor = conn.cursor()

#         print("Connected to Snowflake")

#         # Define the stage and table for loading JSON data
#         stage_name = "sec_stage"
#         table_name = "RAW_JSON_DATA"
        
#         # Create stage if not exists
#         cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")

#         # Upload JSON data to Snowflake stage from S3
#         s3_key = f"sec_data/{year}q{quarter}/processed/data.json"
#         copy_sql = f"""
#             COPY INTO {table_name}
#             FROM @sec_stage/{s3_key}
#             FILE_FORMAT=(TYPE=JSON)
#             ON_ERROR='CONTINUE';
#         """
        
#         cursor.execute(copy_sql)
        
#         cursor.close()
#         conn.close()

#     except Exception as e:
#         print(f"Error loading JSON to Snowflake: {str(e)}")

def load_json_to_snowflake(bucket_name, year, quarter):
    """Load JSON data from S3 into Snowflake."""
    try:
        # Get Snowflake connection from Airflow
        snowflake_conn = BaseHook.get_connection('snowflake_default')

        # Establish connection to Snowflake
        conn = connect(
            user=snowflake_conn.login,
            password=snowflake_conn.password,
            account=snowflake_conn.extra_dejson.get('account'),
            warehouse=snowflake_conn.extra_dejson.get('warehouse'),
            database='SEC_DATA',  # Explicitly define database
            schema='JSON_STAGING'  # Explicitly define schema
        )

        cursor = conn.cursor()

        print("Connected to Snowflake")

        # Define stage and table for loading JSON data
        stage_name = "sec_stage"
        table_name = "RAW_JSON_DATA"

        # Create table if it doesn't exist
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                data VARIANT
            );
        """
        cursor.execute(create_table_sql)
        print(f"Table {table_name} created or already exists.")

        # Define S3 key for the JSON file
        s3_key = f"sec_data/{year}q{quarter}/processed/data.json"

        # Copy JSON data from S3 into Snowflake table
        copy_sql = f"""
            COPY INTO {table_name}
            FROM @sec_stage/{s3_key}
            FILE_FORMAT=(TYPE=JSON)
            ON_ERROR='CONTINUE';
        """
        cursor.execute(copy_sql)
        print(f"Data copied into {table_name} from S3.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error loading JSON to Snowflake: {str(e)}")
        raise
