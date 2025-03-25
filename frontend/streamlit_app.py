# import streamlit as st
# import pandas as pd
# import requests
# from datetime import datetime
# import google.auth
# import google.auth.transport.requests

# # Function to execute SQL query via API
# def execute_query(query, schema="RAW_STAGING"):
#     try:
#         response = requests.post(
#             f"https://finance-data-pipeline.uk.r.appspot.com/api/execute-query",
#             json={"query": query, "schema": schema}
#         )
#         response.raise_for_status()
#         data = response.json().get('data', [])
#         return pd.DataFrame(data)
#     except requests.exceptions.RequestException as e:
#         st.error(f"Error executing query: {str(e)}")
#         return None

# # Function to trigger Cloud Composer DAG via Airflow REST API
# def trigger_composer_dag(dag_id, composer_environment_name, composer_location, project_id, dag_conf=None):
#     credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
#     auth_req = google.auth.transport.requests.AuthorizedSession(credentials)

#     environment_url = (
#         f"https://composer.googleapis.com/v1/projects/{project_id}/locations/{composer_location}/environments/{composer_environment_name}"
#     )

#     response = auth_req.get(environment_url)
#     response.raise_for_status()
#     environment_data = response.json()
#     airflow_uri = environment_data["config"]["airflowUri"]

#     dag_run_url = f"{airflow_uri}/api/v1/dags/{dag_id}/dagRuns"
#     json_data = {"conf": dag_conf or {}}

#     dag_response = auth_req.post(dag_run_url, json=json_data)
#     dag_response.raise_for_status()

#     return dag_response.json()

# # Streamlit page configuration
# st.set_page_config(page_title="SEC Financial Data Explorer", page_icon="📊", layout="wide")

# # Title and description
# st.title("📊 SEC Financial Data Explorer")
# st.markdown("Explore SEC financial data from raw and processed tables")

# # Sidebar for data selection and pipeline triggering
# st.sidebar.header("Data Selection")

# schema_options = {
#     "Raw Data": "RAW_STAGING",
#     "Fact Tables": "FACT_TABLE_STAGING"
# }
# selected_schema = st.sidebar.radio("Select Schema", list(schema_options.keys()))
# current_schema = schema_options[selected_schema]

# # Table selection logic
# if selected_schema == "Raw Data":
#     table_options = {
#         "NUM Table": "RAW_NUM",
#         "PRE Table": "RAW_PRE",
#         "SUB Table": "RAW_SUB",
#         "TAG Table": "RAW_TAG"
#     }
# else:
#     table_type = st.sidebar.radio("Select Table Type", ["Basic Tables", "Financial Tables"])
#     if table_type == "Basic Tables":
#         table_options = {
#             "NUM Table": "NUM",
#             "PRE Table": "PRE",
#             "SUB Table": "SUB",
#             "TAG Table": "TAG"
#         }
#     else:
#         table_options = {
#             "Balance Sheet": "FACT_BALANCE_SHEET",
#             "Cash Flow": "FACT_CASHFLOW",
#             "Income Statement": "FACT_INCOME_STATEMENT"
#         }

# selected_table = st.sidebar.selectbox("Select Table", list(table_options.keys()))
# current_table = table_options[selected_table]

# # Sidebar pipeline trigger section
# st.sidebar.header("Trigger Pipelines")
# pipeline_choice = st.sidebar.radio("Select Pipeline to Trigger", ["SEC Data Pipeline", "JSON Pipeline"])
# trigger_button = st.sidebar.button("🚀 Trigger Selected Pipeline")

# if trigger_button:
#     with st.spinner('Triggering pipeline...'):
#         try:
#             dag_id_map = {
#                 "SEC Data Pipeline": "sec_data_pipeline",
#                 "JSON Pipeline": "json_pipeline"
#             }
#             triggered_dag_id = dag_id_map[pipeline_choice]

#             # Replace these with your Composer details (or use Streamlit secrets)
#             composer_env_name = "<your-composer-environment-name>"
#             composer_region = "<your-composer-region>"  # e.g., us-central1
#             gcp_project_id = "<your-gcp-project-id>"

#             trigger_response = trigger_composer_dag(
#                 dag_id=triggered_dag_id,
#                 composer_environment_name=composer_env_name,
#                 composer_location=composer_region,
#                 project_id=gcp_project_id,
#                 dag_conf={"triggered_by": "streamlit_app"}
#             )

#             run_id_triggered = trigger_response.get('dag_run_id', 'N/A')
#             st.sidebar.success(f"✅ Successfully triggered '{pipeline_choice}' (Run ID: {run_id_triggered})")

#         except Exception as err:
#             st.sidebar.error(f"Error triggering pipeline: {err}")

# # Help section expander
# with st.expander("📚 Need Help? Click here for documentation"):
#     st.markdown("""
# ### Tips:
# - Use `LIMIT` in queries to restrict rows.
# - Join with `SUB` table for company names.
# - Use `LOWER()` for case-insensitive searches.
# """)

# # Footer section
# st.markdown("---")
# st.markdown("<div style='text-align:center'>SEC Financial Data Explorer | Built with Streamlit</div>", unsafe_allow_html=True)

import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime

# API base URL
API_URL = "https://finance-data-pipeline.uk.r.appspot.com"

# Function to execute query
def execute_query(query, schema="RAW_STAGING"):
    try:
        response = requests.post(
            f"{API_URL}/api/execute-query",
            json={
                "query": query,
                "schema": schema
            }
        )
        response.raise_for_status()
        data = response.json().get('data', [])
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Function to trigger pipeline
def trigger_pipeline(pipeline_type):
    try:
        endpoint = "/api/trigger-sec-pipeline" if pipeline_type == "SEC Data Pipeline" else "/api/trigger-json-pipeline"
        response = requests.post(f"{API_URL}{endpoint}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error triggering {pipeline_type}: {str(e)}")
        return None

# Page config
st.set_page_config(
    page_title="SEC Financial Data Explorer",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 SEC Financial Data Explorer")
st.markdown("Explore SEC financial data from raw and processed tables")

# Sidebar for schema and table selection
st.sidebar.header("Data Selection")

# Pipeline Control
st.sidebar.header("Pipeline Control")
pipeline_type = st.sidebar.radio(
    "Select Pipeline Type",
    ["SEC Data Pipeline", "JSON Pipeline"],
    index=0,
    help="Select which pipeline to trigger"
)

if st.sidebar.button(f"🚀 Trigger {pipeline_type}", key="trigger_button"):
    with st.spinner(f"Running {pipeline_type}..."):
        result = trigger_pipeline(pipeline_type)
        if result and result.get('success'):
            st.sidebar.success(f"{pipeline_type} triggered successfully!")
            st.sidebar.json(result.get('execution_id'))
        elif result:
            st.sidebar.error(f"Pipeline error: {result.get('error', 'Unknown error')}")

# Schema selection
schema_options = {
    "Raw Data": "RAW_STAGING",
    "Fact Tables": "FACT_TABLE_STAGING"
}
selected_schema = st.sidebar.radio("Select Schema", list(schema_options.keys()))

# Get current schema based on selection
current_schema = schema_options[selected_schema]

# Table selection based on schema
if selected_schema == "Raw Data":
    table_options = {
        "NUM Table": "RAW_NUM",
        "PRE Table": "RAW_PRE",
        "SUB Table": "RAW_SUB",
        "TAG Table": "RAW_TAG"
    }
    table_options_to_show = table_options
else:  # Fact Tables
    table_type = st.sidebar.radio("Select Table Type", ["Basic Tables", "Financial Tables"])
    
    if table_type == "Basic Tables":
        table_options = {
            "NUM Table": "NUM",
            "PRE Table": "PRE",
            "SUB Table": "SUB",
            "TAG Table": "TAG"
        }
    else:  # Financial Tables
        table_options = {
            "Balance Sheet": "FACT_BALANCE_SHEET",
            "Cash Flow": "FACT_CASHFLOW",
            "Income Statement": "FACT_INCOME_STATEMENT"
        }
    table_options_to_show = table_options

# Select table
selected_table = st.sidebar.selectbox("Select Table", list(table_options_to_show.keys()))

# Get the actual table name
current_table = table_options[selected_table]

# Create default queries based on schema and table type
if selected_schema == "Raw Data":
    query_templates = {
        "View All Columns": f"SELECT * FROM {current_table} LIMIT 100",
        "Sample Query": f"SELECT * FROM {current_table} LIMIT 10",
        "Custom Query": ""
    }
else:  # Fact Tables
    if table_type == "Basic Tables":
        query_templates = {
            "View All Columns": f"SELECT * FROM {current_table} LIMIT 100",
            "Sample Query": f"SELECT * FROM {current_table} LIMIT 10",
            "Custom Query": ""
        }
    else:  # Financial Tables
        query_templates = {
            "Basic View": f"SELECT s.NAME as company_name, f.TAG, f.VALUE, f.STMT, f.PLABEL FROM {current_table} f JOIN SUB s ON f.ADSH = s.ADSH LIMIT 100",
            "Company Search": f"SELECT s.NAME as company_name, f.TAG, f.VALUE, f.STMT, f.PLABEL FROM {current_table} f JOIN SUB s ON f.ADSH = s.ADSH WHERE LOWER(s.NAME) LIKE LOWER('%APPLE%') LIMIT 100",
            "Custom Query": ""
        }

# Template selection
selected_template = st.sidebar.selectbox("Select Query Template", list(query_templates.keys()))

# Main query input
st.subheader("SQL Query")
if selected_template == "Custom Query":
    query = st.text_area(
        "Enter your custom SQL query:",
        height=200,
        placeholder=f"SELECT * FROM {current_table} LIMIT 10"
    )
else:
    query = st.text_area(
        "SQL Query :",
        value=query_templates[selected_template],
        height=200
    )

# Company search box (only for financial tables in fact tables)
if selected_schema == "Fact Tables" and table_type == "Financial Tables" and selected_template == "Company Search":
    company_name = st.text_input("Enter company name to search:")
    if company_name:
        query = query.replace("APPLE", company_name)

# Execute button
col1, col2 = st.columns([1, 6])
with col1:
    execute_button = st.button("Execute Query", type="primary")

if execute_button:
    if query:
        with st.spinner('Executing query...'):
            df = execute_query(query, current_schema)
            
            if df is not None:
                # Display results
                st.subheader("Query Results")
                st.markdown(f"*Found {len(df)} rows*")
                
                # Display the dataframe
                st.dataframe(df, use_container_width=True)
                
                # Download button
                csv = df.to_csv(index=False)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv,
                    file_name=f"query_results_{timestamp}.csv",
                    mime="text/csv",
                    key='download-csv'
                )
                
                # Basic statistics for numerical columns
                num_cols = df.select_dtypes(include=['float64', 'int64']).columns
                if len(num_cols) > 0:
                    with st.expander("View Numerical Statistics"):
                        st.dataframe(df[num_cols].describe(), use_container_width=True)
    else:
        st.warning("Please enter a query")

# Help section
with st.expander("📚 Need Help? Click here for documentation"):
    st.markdown(f"""
    ### Current Selection:
    - Schema: {selected_schema} ({current_schema})
    - Table: {selected_table} ({current_table})
    
    ### Available Tables in Raw Data:
    - `RAW_STAGING.RAW_NUM` - Raw numerical data
    - `RAW_STAGING.RAW_PRE` - Raw presentation data
    - `RAW_STAGING.RAW_SUB` - Raw submission data
    - `RAW_STAGING.RAW_TAG` - Raw tag data
    
    ### Available Tables in Fact Tables:
    #### Basic Tables:
    - `FACT_TABLE_STAGING.NUM` - Numerical data
    - `FACT_TABLE_STAGING.PRE` - Presentation data
    - `FACT_TABLE_STAGING.SUB` - Submission data
    - `FACT_TABLE_STAGING.TAG` - Tag data
    
    #### Financial Tables:
    - `FACT_TABLE_STAGING.FACT_BALANCE_SHEET` - Balance sheet data
    - `FACT_TABLE_STAGING.FACT_CASHFLOW` - Cash flow data
    - `FACT_TABLE_STAGING.FACT_INCOME_STATEMENT` - Income statement data
    
    ### Tips:
    - Use LIMIT to restrict the number of rows returned
    - Join with SUB table to get company names (for financial tables)
    - Use WHERE clause to filter specific companies or metrics
    - Use LOWER() for case-insensitive company name search
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>SEC Financial Data Explorer | Built with Streamlit</p>
</div>
""", unsafe_allow_html=True)


# Sidebar pipeline trigger section
st.sidebar.header("Pipeline Trigger")
pipeline_choice = st.sidebar.radio(
    label="Select Pipeline to Trigger:",
    options=["SEC Data Pipeline", "JSON Pipeline"]
)

trigger_pipeline_button = st.sidebar.button("🚀 Trigger Pipeline")

if trigger_pipeline_button:
    with st.spinner('Triggering pipeline...'):
        try:
            # Map pipeline choice to DAG IDs
            dag_id_map = {
                "SEC Data Pipeline": "sec_data_pipeline",
                "JSON Pipeline": "json_pipeline"
            }
            triggered_dag_id = dag_id_map[pipeline_choice]

            # Replace these with your Composer details
            composer_env_name = "<your-composer-environment-name>"
            composer_region = "<your-composer-region>"  # e.g., us-central1
            gcp_project_id = "<your-gcp-project-id>"

            # Trigger the DAG
            trigger_response = trigger_composer_dag(
                dag_id=triggered_dag_id,
                composer_environment_name=composer_env_name,
                composer_location=composer_region,
                project_id=gcp_project_id,
                dag_conf={"triggered_by": "streamlit_app"}
            )

            run_id_triggered = trigger_response.get('dag_run_id', 'N/A')
            st.sidebar.success(f"✅ Successfully triggered '{pipeline_choice}' (Run ID: {run_id_triggered})")

        except Exception as err:
            st.sidebar.error(f"Error triggering pipeline: {err}")
