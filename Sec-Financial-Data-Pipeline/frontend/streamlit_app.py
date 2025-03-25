# import streamlit as st
# import pandas as pd
# import requests
# import json
# from datetime import datetime

# # Make sure this is at the top of your file and not overwritten anywhere else
# API_URL = "https://finance-data-pipeline.uk.r.appspot.com"

# # Function to execute query
# def execute_query(query, schema="RAW_STAGING"):
#     try:
#         # Print the URL being called (for debugging)
#         full_url = f"{API_URL}/api/execute-query"
#         st.write(f"Calling API at: {full_url}")
        
#         response = requests.post(
#             full_url,
#             json={
#                 "query": query,
#                 "schema": schema
#             }
#         )
#         response.raise_for_status()
#         # Convert the response data to a pandas DataFrame
#         data = response.json().get('data', [])
#         return pd.DataFrame(data)
#     except requests.exceptions.RequestException as e:
#         st.error(f"Error executing query: {str(e)}")
#         st.error(f"API URL being used: {API_URL}")
#         return None

# # Page config
# st.set_page_config(
#     page_title="SEC Financial Data Explorer",
#     page_icon="📊",
#     layout="wide"
# )

# # Title
# st.title("📊 SEC Financial Data Explorer")
# st.markdown("Explore SEC financial data from raw and processed tables")

# # Sidebar for schema and table selection
# st.sidebar.header("Data Selection")

# # Schema selection
# schema_options = {
#     "Raw Data": "RAW_STAGING",
#     "Fact Tables": "FACT_TABLE_STAGING"
# }
# selected_schema = st.sidebar.radio("Select Schema", list(schema_options.keys()))

# # Get current schema based on selection
# current_schema = schema_options[selected_schema]

# # Table selection based on schema
# if selected_schema == "Raw Data":
#     table_options = {
#         "NUM Table": "RAW_NUM",
#         "PRE Table": "RAW_PRE",
#         "SUB Table": "RAW_SUB",
#         "TAG Table": "RAW_TAG"
#     }
#     table_options_to_show = table_options
# else:  # Fact Tables
#     table_type = st.sidebar.radio("Select Table Type", ["Basic Tables", "Financial Tables"])
    
#     if table_type == "Basic Tables":
#         table_options = {
#             "NUM Table": "NUM",
#             "PRE Table": "PRE",
#             "SUB Table": "SUB",
#             "TAG Table": "TAG"
#         }
#     else:  # Financial Tables
#         table_options = {
#             "Balance Sheet": "FACT_BALANCE_SHEET",
#             "Cash Flow": "FACT_CASHFLOW",
#             "Income Statement": "FACT_INCOME_STATEMENT"
#         }
#     table_options_to_show = table_options

# # Select table
# selected_table = st.sidebar.selectbox("Select Table", list(table_options_to_show.keys()))

# # Get the actual table name
# current_table = table_options[selected_table]

# # Create default queries based on schema and table type
# if selected_schema == "Raw Data":
#     query_templates = {
#         "View All Columns": f"""
# SELECT *
# FROM {current_table}
# LIMIT 100
#         """,
#         "Sample Query": f"""
# SELECT *
# FROM {current_table}
# LIMIT 10
#         """,
#         "Custom Query": ""
#     }
# else:  # Fact Tables
#     if table_type == "Basic Tables":
#         query_templates = {
#             "View All Columns": f"""
# SELECT *
# FROM {current_table}
# LIMIT 100
#             """,
#             "Sample Query": f"""
# SELECT *
# FROM {current_table}
# LIMIT 10
#             """,
#             "Custom Query": ""
#         }
#     else:  # Financial Tables
#         query_templates = {
#             "Basic View": f"""
# SELECT 
#     s.NAME as company_name,
#     f.TAG,
#     f.VALUE,
#     f.STMT,
#     f.PLABEL
# FROM FACT_BALANCE_SHEET f
# JOIN SUB s ON f.ADSH = s.ADSH
# LIMIT 100
#             """,
#             "Company Search": f"""
# SELECT 
#     s.NAME as company_name,
#     f.TAG,
#     f.VALUE,
#     f.STMT,
#     f.PLABEL
# FROM FACT_BALANCE_SHEET f
# JOIN SUB s ON f.ADSH = s.ADSH
# WHERE LOWER(s.NAME) LIKE LOWER('%APPLE%')
# LIMIT 100
#             """,
#             "Custom Query": ""
#         }

# # Template selection
# selected_template = st.sidebar.selectbox("Select Query Template", list(query_templates.keys()))

# # Main query input
# st.subheader("SQL Query")
# if selected_template == "Custom Query":
#     query = st.text_area(
#         "Enter your custom SQL query:",
#         height=200,
#         placeholder=f"SELECT * FROM {current_table} LIMIT 10"
#     )
# else:
#     query = st.text_area(
#         "SQL Query :",
#         value=query_templates[selected_template],
#         height=200
#     )

# # Company search box (only for financial tables in fact tables)
# if selected_schema == "Fact Tables" and table_type == "Financial Tables" and selected_template == "Company Search":
#     company_name = st.text_input("Enter company name to search:")
#     if company_name:
#         query = query.replace("APPLE", company_name)

# # Execute button
# col1, col2 = st.columns([1, 6])
# with col1:
#     execute_button = st.button("Execute Query", type="primary")

# if execute_button:
#     if query:
#         with st.spinner('Executing query...'):
#             df = execute_query(query, current_schema)
            
#             if df is not None and not df.empty:
#                 # Display results
#                 st.subheader("Query Results")
#                 st.write(f"Found {len(df)} rows")
                
#                 # Display the dataframe
#                 st.dataframe(df, use_container_width=True)
                
#                 # Download button
#                 if not df.empty:
#                     csv = df.to_csv(index=False)
#                     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#                     st.download_button(
#                         label="📥 Download Results as CSV",
#                         data=csv,
#                         file_name=f"query_results_{timestamp}.csv",
#                         mime="text/csv",
#                         key='download-csv'
#                     )
                
#                 # Basic statistics for numerical columns
#                 num_cols = df.select_dtypes(include=['float64', 'int64']).columns
#                 if len(num_cols) > 0:
#                     with st.expander("View Numerical Statistics"):
#                         st.dataframe(df[num_cols].describe(), use_container_width=True)
#     else:
#         st.warning("Please enter a query")

# # Help section
# with st.expander("📚 Need Help? Click here for documentation"):
#     st.markdown(f"""
#     ### Current Selection:
#     - Schema: {selected_schema} ({current_schema})
#     - Table: {selected_table} ({current_table})
    
#     ### Available Tables in Raw Data:
#     - `RAW_STAGING.RAW_NUM` - Raw numerical data
#     - `RAW_STAGING.RAW_PRE` - Raw presentation data
#     - `RAW_STAGING.RAW_SUB` - Raw submission data
#     - `RAW_STAGING.RAW_TAG` - Raw tag data
    
#     ### Available Tables in Fact Tables:
#     #### Basic Tables:
#     - `FACT_TABLE_STAGING.NUM` - Numerical data
#     - `FACT_TABLE_STAGING.PRE` - Presentation data
#     - `FACT_TABLE_STAGING.SUB` - Submission data
#     - `FACT_TABLE_STAGING.TAG` - Tag data
    
#     #### Financial Tables:
#     - `FACT_TABLE_STAGING.FACT_BALANCE_SHEET` - Balance sheet data
#     - `FACT_TABLE_STAGING.FACT_CASHFLOW` - Cash flow data
#     - `FACT_TABLE_STAGING.FACT_INCOME_STATEMENT` - Income statement data
    
#     ### Tips:
#     - Use LIMIT to restrict the number of rows returned
#     - Join with SUB table to get company names (for financial tables)
#     - Use WHERE clause to filter specific companies or metrics
#     - Use LOWER() for case-insensitive company name search
#     """)

# # Footer
# st.markdown("---")
# st.markdown("""
# <div style='text-align: center'>
#     <p>SEC Financial Data Explorer | Built with Streamlit</p>
# </div>
# """, unsafe_allow_html=True)
















# import streamlit as st
# import pandas as pd
# import requests
# import json
# from datetime import datetime

# # API URL
# API_URL = "https://finance-data-pipeline.uk.r.appspot.com"

# # Function to execute query
# def execute_query(query, schema="RAW_STAGING"):
#     try:
#         response = requests.post(f"{API_URL}/api/execute-query", json={"query": query, "schema": schema})
#         response.raise_for_status()
#         return pd.DataFrame(response.json().get('data', []))
#     except requests.exceptions.RequestException as e:
#         st.error(f"Error executing query: {str(e)}")
#         return None

# # Page Config
# st.set_page_config(page_title="SEC Financial Data Explorer", page_icon="📊", layout="wide")

# st.title("📊 SEC Financial Data Explorer")
# st.markdown("Explore SEC financial data from raw and processed tables.")

# # Sidebar - Organized Sections
# with st.sidebar:
#     st.header("📅 Select Time Period")
    
#     # Year selection dropdown from 2020 to the current year
#     current_year = datetime.now().year
#     selected_year = st.selectbox("Select Year", list(range(2020, current_year + 1))[::-1])

#     # Quarter selection dropdown (1-4)
#     selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

#     st.header("🔍 Data Selection")

#     # Schema Selection
#     schema_options = {"Raw Data": "RAW_STAGING", "Fact Tables": "FACT_TABLE_STAGING"}
#     selected_schema = st.radio("Select Schema", list(schema_options.keys()))
#     current_schema = schema_options[selected_schema]

#     # Table Selection
#     if selected_schema == "Raw Data":
#         table_options = {
#             "NUM Table": "RAW_NUM",
#             "PRE Table": "RAW_PRE",
#             "SUB Table": "RAW_SUB",
#             "TAG Table": "RAW_TAG"
#         }
#         table_type = None  
#     else:
#         table_type = st.radio("Select Table Type", ["Basic Tables", "Financial Tables"])
#         if table_type == "Basic Tables":
#             table_options = {
#                 "NUM Table": "NUM",
#                 "PRE Table": "PRE",
#                 "SUB Table": "SUB",
#                 "TAG Table": "TAG"
#             }
#         else:
#             table_options = {
#                 "Balance Sheet": "FACT_BALANCE_SHEET",
#                 "Cash Flow": "FACT_CASHFLOW",
#                 "Income Statement": "FACT_INCOME_STATEMENT"
#             }

#     selected_table = st.selectbox("Select Table", list(table_options.keys()))
#     current_table = table_options[selected_table]

#     # **Pipeline Selection**
#     st.header("🚀 Select Data Pipeline")
#     pipeline_options = {
#         "SEC Data Pipeline": "SEC_PIPELINE",
#         "JSON Pipeline": "JSON_PIPELINE"
#     }
#     selected_pipeline = st.selectbox("Select Pipeline", list(pipeline_options.keys()))

#     # **Look for Data Button**
#     if st.button("🔍 Look for Data"):
#         st.success(f"Searching data for {selected_year} Q{selected_quarter} using {selected_pipeline}...")

# # **Query Template Selection**
# query_templates = {
#     "View All Columns": f"SELECT * FROM {current_table} LIMIT 100",
#     "Sample Query": f"SELECT * FROM {current_table} LIMIT 10",
#     "Custom Query": ""
# }
# selected_template = st.selectbox("Select Query Template", list(query_templates.keys()))

# # **Main Section**
# st.subheader("SQL Query")

# query = query_templates[selected_template] if selected_template != "Custom Query" else ""

# # **Apply Year & Quarter Filters in Financial Tables**
# if selected_schema == "Fact Tables" and table_type == "Financial Tables":
#     query += f"\nWHERE YEAR = {selected_year} AND QUARTER = {selected_quarter}"

# # **SQL Query Box**
# query = st.text_area("SQL Query:", value=query, height=200)

# # **Execute Query Button**
# if st.button("🚀 Execute Query"):
#     if query:
#         with st.spinner('Executing query...'):
#             df = execute_query(query, current_schema)
#             if df is not None and not df.empty:
#                 st.subheader("Query Results")
#                 st.write(f"Found {len(df)} rows")
#                 st.dataframe(df, use_container_width=True)

#                 # Download CSV
#                 csv = df.to_csv(index=False)
#                 timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#                 st.download_button(
#                     label="📥 Download Results as CSV",
#                     data=csv,
#                     file_name=f"query_results_{timestamp}.csv",
#                     mime="text/csv"
#                 )

#                 # Show basic statistics for numeric columns
#                 num_cols = df.select_dtypes(include=['float64', 'int64']).columns
#                 if len(num_cols) > 0:
#                     with st.expander("View Numerical Statistics"):
#                         st.dataframe(df[num_cols].describe(), use_container_width=True)
#     else:
#         st.warning("Please enter a query.")

# # **Help Section**
# with st.expander("📚 Need Help? Click here for documentation"):
#     st.markdown(f"""
#     **Current Selection:**
#     - Schema: {selected_schema} ({current_schema})
#     - Table: {selected_table} ({current_table})

#     **Financial Table Filters:**
#     - Year: {selected_year}
#     - Quarter: Q{selected_quarter}
#     - Pipeline: {selected_pipeline}

#     **Example SQL filter:**  
#     ```sql
#     WHERE YEAR = {selected_year} AND QUARTER = {selected_quarter}
#     ```
#     """)

# # **Footer**
# st.markdown("---")
# st.markdown("<div style='text-align: center'><p>SEC Financial Data Explorer | Built with Streamlit</p></div>", unsafe_allow_html=True)






import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# API URL
API_URL = "https://finance-data-pipeline.uk.r.appspot.com"

# Function to execute query
def execute_query(query, schema="RAW_STAGING"):
    try:
        response = requests.post(f"{API_URL}/api/execute-query", json={"query": query, "schema": schema})
        response.raise_for_status()
        return pd.DataFrame(response.json().get('data', []))
    except requests.exceptions.RequestException as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Page Config
st.set_page_config(page_title="SEC Financial Data Explorer", page_icon="📊", layout="wide")

st.title("📊 SEC Financial Data Explorer")
st.markdown("Explore SEC financial data from raw and processed tables.")

# **Sidebar - Year, Quarter & Pipeline Selection**
with st.sidebar:
    st.header("📅 Select Time Period")
    
    # Year selection from 2020 to current year
    current_year = datetime.now().year
    selected_year = st.selectbox("Select Year", options=list(range(2020, current_year + 1)), index=current_year - 2020)

    # Quarter selection (1 to 4)
    selected_quarter = st.selectbox("Select Quarter", options=[1, 2, 3, 4])

    st.header("🔗 Select Pipeline")
    pipeline_options = {
        "SEC Data Pipeline": "SEC_PIPELINE",
        "JSON Pipeline": "JSON_PIPELINE"
    }
    selected_pipeline = st.radio("Choose Pipeline", list(pipeline_options.keys()))

# **Look for Data Button**
st.markdown("---")
if st.button("🔍 Look for Data"):
    st.write(f"Fetching data for **Year {selected_year}, Quarter {selected_quarter}** using **{selected_pipeline}**...")

    # Constructing query (example)
    query = f"SELECT * FROM FACT_BALANCE_SHEET WHERE YEAR = {selected_year} AND QUARTER = {selected_quarter} LIMIT 100"
    
    with st.spinner("Executing query..."):
        df = execute_query(query, "FACT_TABLE_STAGING")
        if df is not None and not df.empty:
            st.subheader("Query Results")
            st.write(f"Found {len(df)} rows")
            st.dataframe(df, use_container_width=True)

            # Download CSV
            csv = df.to_csv(index=False)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv,
                file_name=f"query_results_{timestamp}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No data found for the selected criteria.")

# **Footer**
st.markdown("---")
st.markdown("<div style='text-align: center'><p>SEC Financial Data Explorer | Built with Streamlit</p></div>", unsafe_allow_html=True)
