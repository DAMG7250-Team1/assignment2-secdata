# import requests
# from bs4 import BeautifulSoup
# import os
# import boto3
# import time
# import pandas as pd
# from zipfile import ZipFile
# import logging
# import urllib3
# from airflow.hooks.base import BaseHook
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import shutil

# logger = logging.getLogger(__name__)

# def download_sec_data(year, quarter, **context):
#     """Download SEC financial statement data sets"""
#     try:
#         # Direct download URL
#         download_url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
        
#         headers = {
#             'User-Agent': 'Sample Company Name AdminContact@company.com',
#             'Accept-Encoding': 'gzip, deflate',
#             'Host': 'www.sec.gov'
#         }
        
#         logger.info(f"Attempting to download from: {download_url}")
        
#         # Create directory
#         os.makedirs("./data/financial_statement_data_sets", exist_ok=True)
        
#         # Download file
#         local_filename = f"./data/financial_statement_data_sets/{year}q{quarter}.zip"
        
#         # Add delay to respect SEC rate limits
#         time.sleep(0.1)
        
#         response = requests.get(
#             download_url, 
#             headers=headers,
#             allow_redirects=True
#         )
        
#         logger.info(f"Download status code: {response.status_code}")
        
#         if response.status_code == 200:
#             with open(local_filename, 'wb') as f:
#                 f.write(response.content)
#             logger.info(f"Successfully downloaded {year}q{quarter}.zip")
#             return [[local_filename, f"{year}q{quarter}.zip"]]
#         else:
#             raise ValueError(f"Download failed with status code: {response.status_code}")
            
#     except Exception as e:
#         logger.error(f"Error downloading SEC data: {str(e)}")
#         raise

# def upload_to_s3(downloaded_files, year, quarter, **context):
#     """Upload zip and extracted files to S3"""
#     try:
#         logger.info(f"Starting upload_to_s3 with downloaded_files: {downloaded_files}")
        
#         # Ensure path exists
#         zip_path = f"./data/financial_statement_data_sets/{year}q{quarter}.zip"
#         if not os.path.exists(zip_path):
#             raise FileNotFoundError(f"Expected file not found: {zip_path}")

#         logger.info(f"Processing zip file: {zip_path}")


#         bucket_name = "sec-finance-data-team1"
#         temp_dir = "/tmp/sec_data"
        
#         # Create temp directory
#         os.makedirs(temp_dir, mode=0o777, exist_ok=True)
#         logger.info(f"Created temp directory: {temp_dir}")
        
#         # Create S3 client
#         aws_conn = BaseHook.get_connection('aws_default')
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_conn.login,
#             aws_secret_access_key=aws_conn.password,
#             region_name='us-east-1'
#         )
        
#         # Handle the downloaded_files parameter
#         if isinstance(downloaded_files, str):
#             import ast
#             downloaded_files = ast.literal_eval(downloaded_files)
        
#         zip_path = downloaded_files[0][0]
#         logger.info(f"Processing zip file: {zip_path}")
        
#         # Extract and upload files
#         with ZipFile(zip_path) as zip_file:
#             for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
#                 try:
#                     # Extract file to temp directory
#                     temp_file = os.path.join(temp_dir, filename)
#                     logger.info(f"Extracting {filename} to {temp_file}")
                    
#                     with zip_file.open(filename) as source, open(temp_file, 'wb') as target:
#                         target.write(source.read())
                    
#                     # Upload to S3
#                     s3_key = f"sec_data/{year}q{quarter}/raw/{filename}"
#                     s3_client.upload_file(temp_file, bucket_name, s3_key)
#                     logger.info(f"Successfully uploaded {filename} to S3: {s3_key}")
                    
#                     # Clean up temp file
#                     os.remove(temp_file)
#                     logger.info(f"Cleaned up temp file: {temp_file}")
                    
#                 except Exception as e:
#                     logger.error(f"Error uploading {filename}: {str(e)}")
#                     raise

#         # Clean up
#         shutil.rmtree(temp_dir, ignore_errors=True)
#         shutil.rmtree("./data/financial_statement_data_sets", ignore_errors=True)
#         logger.info("Cleaned up all temporary directories")
        
#         return True

#     except Exception as e:
#         logger.error(f"Error in upload_to_s3: {str(e)}")
#         raise

# def process_and_load_to_snowflake(database, schema, **context):
#     """Process and load data to Snowflake"""
#     def execute_with_context(snow_hook, sql):
#         """Execute SQL with proper database and schema context"""
#         setup_sql = f"""
#         USE DATABASE {database};
#         USE SCHEMA {schema};
#         {sql}
#         """
#         return snow_hook.run(setup_sql)

#     try:
#         # Get AWS connection
#         aws_conn = BaseHook.get_connection('aws_default')
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_conn.login,
#             aws_secret_access_key=aws_conn.password,
#             region_name='us-east-1'
#         )

#         # Create temp directory
#         temp_dir = "/tmp"
        
#         # Get Snowflake hook
#         snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
#         # First ensure schema exists
#         execute_with_context(snow_hook, f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
#         # Create stage
#         execute_with_context(snow_hook, "CREATE STAGE IF NOT EXISTS sec_stage")
        
#         # Process each file
#         files = {
#             'RAW_NUM': 'num.txt',
#             'RAW_PRE': 'pre.txt',
#             'RAW_SUB': 'sub.txt',
#             'RAW_TAG': 'tag.txt'
#         }
        
#         for table_name, filename in files.items():
#             try:
#                 # Create table if not exists
#                 if table_name == 'RAW_NUM':
#                     create_table_sql = f"""
#                     CREATE TABLE IF NOT EXISTS {table_name} (
#                         adsh VARCHAR,
#                         tag VARCHAR,
#                         version VARCHAR,
#                         coreg VARCHAR,
#                         ddate VARCHAR,
#                         qtrs VARCHAR,
#                         uom VARCHAR,
#                         value VARCHAR,
#                         footnote VARCHAR
#                     )
#                     """
#                     execute_with_context(snow_hook, create_table_sql)
                
#                 # Download from S3
#                 logger.info(f"Downloading sec_data/2023q4/raw/{filename} from S3...")
                
#                 temp_file = f"{temp_dir}/{filename}"
#                 s3_client.download_file('sec-edgar-filings', f'sec_data/2023q4/raw/{filename}', temp_file)
                
#                 # Read file to get record count
#                 df = pd.read_csv(temp_file, sep='\t', nrows=1)
#                 logger.info(f"Read {filename} from S3: {len(df)} records")
                
#                 # Put file to stage
#                 put_sql = f"PUT file://{temp_file} @sec_stage AUTO_COMPRESS=TRUE"
#                 execute_with_context(snow_hook, put_sql)
                
#                 # Copy into table
#                 copy_sql = f"""
#                 COPY INTO {table_name}
#                 FROM @sec_stage/{filename}
#                 FILE_FORMAT = (
#                     TYPE = CSV 
#                     FIELD_DELIMITER = '\t'
#                     SKIP_HEADER = 1
#                     ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
#                     EMPTY_FIELD_AS_NULL = TRUE
#                     REPLACE_INVALID_CHARACTERS = TRUE
#                 )
#                 ON_ERROR = 'CONTINUE'
#                 """
#                 execute_with_context(snow_hook, copy_sql)
                
#                 # Remove staged file
#                 execute_with_context(snow_hook, f"REMOVE @sec_stage/{filename}")
                
#                 # Clean up temp file
#                 os.remove(temp_file)
#                 logger.info(f"Successfully processed {filename}")
                
#             except Exception as e:
#                 logger.error(f"Error processing {filename}: {str(e)}")
#                 if os.path.exists(temp_file):
#                     os.remove(temp_file)
#                 raise
        
#         logger.info("Successfully processed all files")
#         return True
        
#     except Exception as e:
#         logger.error(f"Error in process_and_load_to_snowflake: {str(e)}")
#         raise


# import requests
# from bs4 import BeautifulSoup
# import os
# import boto3
# import time
# import pandas as pd
# from zipfile import ZipFile
# import logging
# import shutil
# from airflow.hooks.base import BaseHook
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# logger = logging.getLogger(__name__)

# def download_sec_data(year, quarter):
#     """Download SEC financial statement data sets"""
#     try:
#         download_url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
#         headers = {
#             'User-Agent': 'Sample Company Name AdminContact@company.com',
#             'Accept-Encoding': 'gzip, deflate',
#             'Host': 'www.sec.gov'
#         }
#         local_filename = f"./data/financial_statement_data_sets/{year}q{quarter}.zip"
#         os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        
#         time.sleep(0.1)  # Rate limit compliance
#         response = requests.get(download_url, headers=headers, allow_redirects=True)
        
#         if response.status_code == 200:
#             with open(local_filename, 'wb') as f:
#                 f.write(response.content)
#             result = [[local_filename, f"{year}q{quarter}.zip"]]
#             logger.info(f"Downloaded files structure: {result}")
#             return result  # Ensure it's a nested list
#         else:
#             raise ValueError(f"Download failed with status code: {response.status_code}")
#     except Exception as e:
#         logger.error(f"Error downloading SEC data: {str(e)}")
#         raise




# logger = logging.getLogger(__name__)

# def upload_to_s3(downloaded_files, year, quarter, aws_access_key, aws_secret_key, region_name='us-east-1'):
#     """Upload SEC data to S3"""
#     try:
#         logger.info(f"downloaded_files: {downloaded_files}")  # Debugging print
#         # Corrected validation
#         if (
#             not isinstance(downloaded_files, list) 
#             or not downloaded_files 
#             or not all(isinstance(i, list) and len(i) == 2 and isinstance(i[0], str) and isinstance(i[1], str) for i in downloaded_files)
#         ):
#             raise ValueError(f"Invalid structure of downloaded_files: {downloaded_files}. Expected a nested list with [filepath, filename] as strings.")



#         bucket_name = "sec-finance-data-team1"
#         temp_dir = "/tmp/sec_data"
#         os.makedirs(temp_dir, exist_ok=True)
        
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name=region_name
#         )

#         zip_path = downloaded_files[0][0]  # Ensure this is a valid file path
#         if not os.path.exists(zip_path):
#             raise FileNotFoundError(f"Zip file does not exist: {zip_path}")

#         with ZipFile(zip_path) as zip_file:
#             for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
#                 temp_file = os.path.join(temp_dir, filename)
#                 with zip_file.open(filename) as source, open(temp_file, 'wb') as target:
#                     target.write(source.read())
#                 s3_key = f"sec_data/{year}q{quarter}/raw/{filename}"
#                 s3_client.upload_file(temp_file, bucket_name, s3_key)
#                 os.remove(temp_file)
#         shutil.rmtree(temp_dir, ignore_errors=True)
#         return True
#     except Exception as e:
#         logger.error(f"Error uploading to S3: {str(e)}")
#         raise
















# import requests
# import os
# import boto3
# import time
# from zipfile import ZipFile
# import logging
# import shutil
# import ast
# from airflow.hooks.base import BaseHook

# logger = logging.getLogger(__name__)

# def download_sec_data(year, quarter, **context):
#     """Download SEC financial statement data sets"""
#     try:
#         download_url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
#         headers = {
#             'User-Agent': 'Sample Company Name AdminContact@company.com',
#             'Accept-Encoding': 'gzip, deflate',
#             'Host': 'www.sec.gov'
#         }
#         local_filename = f"./data/financial_statement_data_sets/{year}q{quarter}.zip"
#         os.makedirs(os.path.dirname(local_filename), exist_ok=True)
#         time.sleep(0.1)
#         response = requests.get(download_url, headers=headers, allow_redirects=True)
#         if response.status_code == 200:
#             with open(local_filename, 'wb') as f:
#                 f.write(response.content)
#             result = [[local_filename, f"{year}q{quarter}.zip"]]
#             logger.info(f"Downloaded files structure: {result}")
#             return result
#         else:
#             raise ValueError(f"Download failed with status code: {response.status_code}")
#     except Exception as e:
#         logger.error(f"Connection test failed: {str(e)}")
#         raise

# def upload_to_s3(downloaded_files, year, quarter, aws_access_key, aws_secret_key, region_name='us-east-1'):
#     """Upload SEC data to S3"""
#     try:
#         if isinstance(downloaded_files, str):
#             downloaded_files = ast.literal_eval(downloaded_files)

#         bucket_name = "damg7245-assignment2-team-1"
#         temp_dir = "/tmp/sec_data"
#         os.makedirs(temp_dir, exist_ok=True)

#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name=region_name
#         )

#         zip_path = downloaded_files[0][0]
#         if not os.path.exists(zip_path):
#             raise FileNotFoundError(f"Zip file does not exist: {zip_path}")

#         with ZipFile(zip_path) as zip_file:
#             for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
#                 temp_file = os.path.join("/tmp", filename)
#                 with zip_file.open(filename) as source, open(temp_file, 'wb') as target:
#                     target.write(source.read())

#                 s3_key = f"sec_data/{year}q{quarter}/raw/{filename}"
#                 s3_client.upload_file(temp_file, "damg7245-assignment2-team-1", s3_key)
#                 os.remove(temp_file)

#         shutil.rmtree("/tmp/sec_data", ignore_errors=True)
#         shutil.rmtree("./data/financial_statement_data_sets", ignore_errors=True)
#         return True
#     except Exception as e:
#         logger.error(f"Error uploading to S3: {str(e)}")
#         raise



# def process_and_load_to_snowflake(database, schema, **context):
#     """Load SEC data from S3 to Snowflake"""
#     def execute_with_context(snow_hook, sql):
#         setup_sql = f"""
#         USE DATABASE {database};
#         USE SCHEMA {schema};
#         {sql}
#         """
#         return snow_hook.run(setup_sql)
#     try:
#         aws_conn = BaseHook.get_connection('aws_default')
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_conn.login,
#             aws_secret_access_key=aws_conn.password,
#             region_name='us-east-1'
#         )
#         temp_dir = "/tmp"
#         snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
#         execute_with_context(snow_hook, f"CREATE SCHEMA IF NOT EXISTS {schema}")
#         execute_with_context(snow_hook, "CREATE STAGE IF NOT EXISTS sec_stage")
#         files = {'RAW_NUM': 'num.txt', 'RAW_PRE': 'pre.txt', 'RAW_SUB': 'sub.txt', 'RAW_TAG': 'tag.txt'}
        
#         for table_name, filename in files.items():
#             try:
#                 create_table_sql = f"""
#                 CREATE TABLE IF NOT EXISTS {table_name} (
#                     adsh VARCHAR, tag VARCHAR, version VARCHAR,
#                     coreg VARCHAR, ddate VARCHAR, qtrs VARCHAR,
#                     uom VARCHAR, value VARCHAR, footnote VARCHAR
#                 )"""
#                 execute_with_context(snow_hook, create_table_sql)
#                 temp_file = f"{temp_dir}/{filename}"
#                 s3_client.download_file('sec-edgar-filings', f'sec_data/2023q4/raw/{filename}', temp_file)
#                 put_sql = f"PUT file://{temp_file} @sec_stage AUTO_COMPRESS=TRUE"
#                 execute_with_context(snow_hook, put_sql)
#                 copy_sql = f"""
#                 COPY INTO {table_name} FROM @sec_stage/{filename}
#                 FILE_FORMAT = (
#                     TYPE = CSV, FIELD_DELIMITER = '\t', SKIP_HEADER = 1,
#                     ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
#                     EMPTY_FIELD_AS_NULL = TRUE, REPLACE_INVALID_CHARACTERS = TRUE
#                 ) ON_ERROR = 'CONTINUE'"""
#                 execute_with_context(snow_hook, copy_sql)
#                 execute_with_context(snow_hook, f"REMOVE @sec_stage/{filename}")
#                 os.remove(temp_file)
#             except Exception as e:
#                 logger.error(f"Error processing {filename}: {str(e)}")
#                 if os.path.exists(temp_file):
#                     os.remove(temp_file)
#                 raise
#         return True
#     except Exception as e:
#         logger.error(f"Error in process_and_load_to_snowflake: {str(e)}")
#         raise



import requests
import os
import boto3
import time
from zipfile import ZipFile
import logging
import shutil
import ast
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # <-- Added import here

logger = logging.getLogger(__name__)

def download_sec_data(year, quarter, **context):
    try:
        download_url =  f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
        headers = {
            'User-Agent': 'Sample Company Name AdminContact@company.com',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        local_filename = f"./data/financial_statement_data_sets/{year}/q{quarter}.zip"
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        time.sleep(0.1)
        response = requests.get(download_url, headers=headers, allow_redirects=True)
        if response.status_code == 200:
            with open(local_filename, 'wb') as f:
                f.write(response.content)
            result = [[local_filename, f"{year}q{quarter}.zip"]]
            logger.info(f"Downloaded files structure: {result}")
            return result
        else:
            raise ValueError(f"Download failed with status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error downloading SEC data: {str(e)}")
        raise

def upload_to_s3(downloaded_files, year, quarter, aws_access_key, aws_secret_key, region_name='us-east-1'):
    import ast
    try:
        if isinstance(downloaded_files, str):
            downloaded_files = ast.literal_eval(downloaded_files)

        bucket_name = "damg7245-assignment2-team-1"
        temp_dir = "/tmp/sec_data"
        os.makedirs(temp_dir, exist_ok=True)

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )

        zip_path = downloaded_files[0][0]
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"Zip file does not exist: {zip_path}")

        with ZipFile(zip_path) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                temp_file = os.path.join("/tmp", filename)
                with zip_file.open(filename) as source, open(temp_file, 'wb') as target:
                    target.write(source.read())

                s3_key = f"sec_data/{year}/q{quarter}/raw/{filename}"
                s3_client.upload_file(temp_file, bucket_name, s3_key)
                os.remove(temp_file)

        shutil.rmtree("/tmp/sec_data", ignore_errors=True)
        shutil.rmtree("./data/financial_statement_data_sets", ignore_errors=True)
        return True
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

# def process_and_load_to_snowflake(database, schema, quater, year, **context):
#     """Load SEC data from S3 to Snowflake"""

#     def execute_with_context(snow_hook, sql):
#         """Execute SQL with proper database and schema context"""
#         setup_sql = f"""
#         USE DATABASE "{database}";
#         USE SCHEMA "{schema}";
#         {sql}
#         """
#         return snow_hook.run(setup_sql)

#     try:
#         # Get AWS connection
#         aws_conn = BaseHook.get_connection('aws_default')
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_conn.login,
#             aws_secret_access_key=aws_conn.password,
#             region_name='us-east-1'
#         )

#         temp_dir = "/tmp"
#         snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

#         # Ensure schema exists
#         execute_with_context(snow_hook, f"CREATE SCHEMA IF NOT EXISTS \"{schema}\"")

#         # Create stage
#         execute_with_context(snow_hook, "CREATE STAGE IF NOT EXISTS sec_stage")

#         # Process each file
#         files = {'RAW_NUM': 'num.txt', 'RAW_PRE': 'pre.txt', 'RAW_SUB': 'sub.txt', 'RAW_TAG': 'tag.txt'}
#         for table_name, filename in files.items():
#             try:
#                 # Create table if not exists
#                 create_table_sql = f"""
#                 CREATE TABLE IF NOT EXISTS "{table_name}" (
#                     adsh VARCHAR,
#                     tag VARCHAR,
#                     version VARCHAR,
#                     coreg VARCHAR,
#                     ddate VARCHAR,
#                     qtrs VARCHAR,
#                     uom VARCHAR,
#                     value VARCHAR,
#                     footnote VARCHAR
#                 )
#                 """
#                 execute_with_context(snow_hook, create_table_sql)

#                 # Download from S3
#                 temp_file = f"{temp_dir}/{filename}"
#                 s3_client.download_file('damg7245-assignment2-team-1', f'sec_data/{year}/q{quater}/raw/{filename}', temp_file)

#                 # Put file to stage
#                 put_sql = f"PUT file://{temp_file} @sec_stage AUTO_COMPRESS=TRUE"
#                 execute_with_context(snow_hook, put_sql)

#                 # Copy into table
#                 copy_sql = f"""
#                 COPY INTO "{table_name}"
#                 FROM @sec_stage/{filename}
#                 FILE_FORMAT = (
#                     TYPE = CSV FIELD_DELIMITER='\t' SKIP_HEADER=1 
#                     ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE 
#                     EMPTY_FIELD_AS_NULL=TRUE REPLACE_INVALID_CHARACTERS=TRUE
#                 )
#                 ON_ERROR='CONTINUE'
#                 """
#                 execute_with_context(snow_hook, copy_sql)

#                 # Remove staged file
#                 execute_with_context(snow_hook, f"REMOVE @sec_stage/{filename}")

#                 # Clean up temp file
#                 os.remove(temp_file)
#             except Exception as e:
#                 logger.error(f"Error processing {filename}: {str(e)}")
#                 if os.path.exists(temp_file):
#                     os.remove(temp_file)
#                 raise

#         logger.info("Successfully processed all files")
#         return True

#     except Exception as e:
#         logger.error(f"Error in process_and_load_to_snowflake: {str(e)}")
#         raise



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

import boto3
import tempfile
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def process_and_load_to_snowflake(database, schema, quarter, year, **context):
    """Load SEC data from S3 to Snowflake with year and quarter in table names"""

    def execute_with_context(snow_hook, sql):
        setup_sql = f"""
        USE DATABASE "{database}";
        USE SCHEMA "{schema}";
        {sql}
        """
        return snow_hook.run(setup_sql)

    try:
        # Get AWS connection
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )

        # Initialize Snowflake connection
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

        # Ensure schema and stage exist
        execute_with_context(snow_hook, f"CREATE SCHEMA IF NOT EXISTS \"{schema}\"")
        execute_with_context(snow_hook, "CREATE STAGE IF NOT EXISTS sec_stage")

        # Define the files and their corresponding table prefixes
        files = {
            'RAW_NUM': 'num.txt',
            'RAW_PRE': 'pre.txt',
            'RAW_SUB': 'sub.txt',
            'RAW_TAG': 'tag.txt'
        }

        # Loop through each file and process it
        for table_prefix, filename in files.items():
            table_name = f"{table_prefix}_{year}_{quarter}"
            try:
                # Create table if it doesn't exist
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    adsh VARCHAR,
                    tag VARCHAR,
                    version VARCHAR,
                    coreg VARCHAR,
                    ddate VARCHAR,
                    qtrs VARCHAR,
                    uom VARCHAR,
                    value VARCHAR,
                    footnote VARCHAR
                )
                """
                execute_with_context(snow_hook, create_table_sql)

                # Download file from S3 to a temporary file
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    logger.info(f"Downloading {filename} to temporary file: {temp_file.name}")
                    
                    s3_client.download_file(
                        'damg7245-assignment2-team-1',
                        f'sec_data/{year}/q{quarter}/raw/{filename}',
                        temp_file.name
                    )

                    # Upload file to Snowflake stage
                    put_sql = f"PUT file://{temp_file.name} @sec_stage AUTO_COMPRESS=TRUE"
                    execute_with_context(snow_hook, put_sql)

                    # Copy data into the Snowflake table
                    copy_sql = f"""
                    COPY INTO "{table_name}"
                    FROM @sec_stage/{filename}
                    FILE_FORMAT = (
                        TYPE = CSV FIELD_DELIMITER='\t' SKIP_HEADER=1 
                        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE 
                        EMPTY_FIELD_AS_NULL=TRUE REPLACE_INVALID_CHARACTERS=TRUE
                    )
                    ON_ERROR='CONTINUE'
                    """
                    execute_with_context(snow_hook, copy_sql)

                    # Remove the file from the Snowflake stage
                    execute_with_context(snow_hook, f"REMOVE @sec_stage/{filename}")

                # Clean up the temporary file
                os.remove(temp_file.name)
                logger.info(f"Temporary file {temp_file.name} deleted.")

            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                if os.path.exists(temp_file.name):
                    os.remove(temp_file.name)
                raise

        logger.info("Successfully processed all files.")
        return True

    except Exception as e:
        logger.error(f"Error in process_and_load_to_snowflake: {str(e)}")
        raise