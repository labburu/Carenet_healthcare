# Databricks notebook source

storage_account = "carenetdemodatalake"
storage_account_key = dbutils.secrets.get("CarenetADLSScope","carenetadlsaccesskey")


# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account),
    storage_account_key
)

# COMMAND ----------


import sys
sys.path.append("/Workspace/Users/lalita.abburu@bluemantis.com/")
from common_utils.AppInsights import *

import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

def get_tables_dev_list():
    tables_list = ["InteractionSummaryDev","InteractionCustomAttributesDev","Custom_SegmentsParsedDev"]
    return tables_list


def get_tables_prod_list():
    tables_list = ["InteractionSummary","InteractionCustomAttributes","Custom_SegmentsParsed"]
    #tables_list = ["InteractionCustomAttributes","Custom_SegmentsParsed"]
    return tables_list

# COMMAND ----------


import concurrent.futures
def run_incremental_dev_process(method_name):
    # Example input data
    
    tables_list = get_tables_dev_list()
    # Number of worker threads (adjust based on your needs)
    num_threads = len(tables_list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Submit the method for each item in input_data
        futures = [executor.submit(method_name, item) for item in tables_list]

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

# COMMAND ----------


import concurrent.futures
def run_incremental_prod_process(method_name):
    # Example input data
    
    tables_list = get_tables_prod_list()
    # Number of worker threads (adjust based on your needs)
    num_threads = len(tables_list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Submit the method for each item in input_data
        futures = [executor.submit(method_name, item) for item in tables_list]

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

# COMMAND ----------


import concurrent.futures
def run_full_process(method_name):
    # Example input data
    
    tables_list = ["AccountCodeMirror","Facility","Contracts","I3_DNIS_Info_V","rlFacilityContract"]
    # Number of worker threads (adjust based on your needs)
    num_threads = len(tables_list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Submit the method for each item in input_data
        futures = [executor.submit(method_name, item) for item in tables_list]

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

# COMMAND ----------

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
def run_testing_process(method_name, table_name):
    # Example input data
    tables_list = [table_name]
    # Number of worker threads (adjust based on your needs)
    
    executor1 = ThreadPoolExecutor()
    max_threads = executor1._max_workers
    print(f"max threads : {max_threads}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        # Submit the method for each item in input_data
        futures = [executor.submit(method_name, item) for item in tables_list]

        # Wait for all futures to complete
        concurrent.futures.wait(futures)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Log Table

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from databricks.sdk.runtime import *


# Function to check if Delta table path exists
def delta_table_exists(table_path):
    try:
        # Check if the table path exists
        dbutils.fs.ls(table_path)
        # If the table path exists, check if it contains Delta files
        delta_files = dbutils.fs.ls(f"{table_path}/_delta_log")
        if any(file.name.startswith("0000") for file in delta_files):
            return True
        else:
            return False
    except Exception as e:
        # Handle exceptions, such as if the table path doesn't exist
        print(f"Error checking if Delta table exists: {str(e)}")
        return False


# Define data
data = [(datetime(9999, 1, 1, 00, 00, 00), datetime(9999, 12, 31, 11, 59, 59), "full load", "test", "test", "test", "bronze","bronze//", 0.00, "success", 0, datetime(9999, 12, 31, 11, 59, 59))]

# Define schema with appropriate data types
schema = StructType([
    StructField("start_datetime_utc", TimestampType(), True),
    StructField("end_datetime_utc", TimestampType(), True),
    StructField("load_type", StringType(), True),
    StructField("source_server", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("medallion_layer", StringType(), True),
    StructField("data_lake_path", StringType(), True),
    StructField("load_execution_time", FloatType(), True),
    StructField("notebook_execution_status", StringType(), True),
    StructField("records_inserted", IntegerType(), True),
    StructField("last_record_inserted_datetime", TimestampType(), True)
])

# Create DataFrame
log_df = spark.createDataFrame(data, schema=schema)

# Specify the Delta table path
delta_table_path = "abfss://logs@carenetdemodatalake.dfs.core.windows.net/logtable"

if not delta_table_exists(delta_table_path):
    print("Log table does not exist, so creating one...")
    # Create a Delta table if it doesn't exist
    log_df.write.format("delta").mode("overwrite").save(delta_table_path)


# COMMAND ----------

from pyspark.sql.functions import *
from databricks.sdk.runtime import *
from delta.tables import DeltaTable

def update_log_table(df_row_count,last_inserted_datetime, table_path, file_format,notebook_start_time, notebook_end_time, medallion_layer, table_json_data, execution_time, notebook_execution_status):   
    try:    
        container = table_json_data['landing_container']
        storage_account = table_json_data['storage_account']
        host_name = table_json_data['host_name']
        database_name = table_json_data['database_name']
        sql_table = table_json_data['sql_table']
        load_type = table_json_data['load_type'] 
        watermark_column = table_json_data['watermark_column']
        print(f"Start update_log_table ({host_name},{sql_table})")                                
       
        log_table_path = f"abfss://logs@{storage_account}.dfs.core.windows.net/logtable"
        
        notebook_start_time_timestamp = spark.sql(f"SELECT TIMESTAMP '{notebook_start_time}' AS timestamp_column").collect()[0][0]
        notebook_end_time_timestamp = spark.sql(f"SELECT TIMESTAMP '{notebook_end_time}' AS timestamp_column").collect()[0][0]
       
        last_inserted_datetime_timestamp = None

        if load_type == "incremental":
            if df_row_count > 0:
                if last_inserted_datetime is not None:
                    last_inserted_datetime_timestamp = spark.sql(f"SELECT TIMESTAMP '{last_inserted_datetime}' AS timestamp_column").collect()[0][0]
                else:
                    print(f"last_inserted_datetime is null for {host_name}, {sql_table}")
            else:
                print(f" row count is 0 for {host_name}, {sql_table}")
        else:
            last_inserted_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            last_inserted_datetime_timestamp = spark.sql(f"SELECT TIMESTAMP '{last_inserted_datetime}' AS timestamp_column").collect()[0][0]         
            
        #print(f"last_inserted_datetime {last_inserted_datetime}")
        #print(f"watermark_column {watermark_column}"
            
        new_log_row_schema = StructType([
            StructField("start_datetime_utc", TimestampType(), True),
            StructField("end_datetime_utc", TimestampType(), True),
            StructField("load_type", StringType(), True),
            StructField("source_server", StringType(), True),
            StructField("source_database", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("medallion_layer", StringType(), True),
            StructField("data_lake_path", StringType(), True),
            StructField("load_execution_time", FloatType(), True),
            StructField("notebook_execution_status", StringType(), True),
            StructField("records_inserted", IntegerType(), True),
            StructField("last_record_inserted_datetime", TimestampType(), True)
        ])
        # Step 3: Create a DataFrame with the new row data
        new_log_row_data = [(
                        notebook_start_time_timestamp, 
                        notebook_end_time_timestamp, 
                        load_type, 
                        host_name, 
                        database_name, 
                        sql_table, 
                        medallion_layer,
                        table_path, 
                        execution_time,
                        notebook_execution_status,
                        df_row_count,
                        last_inserted_datetime_timestamp)]
        new_log_row_df = spark.createDataFrame(new_log_row_data, schema=new_log_row_schema)
        # Step 5: Write the new row to the existing Delta table with mode("append")
        log_write_start_time = datetime.now()
        new_log_row_df.write.format("delta").mode("append").save(log_table_path)
        log_write_end_time = datetime.now()
        log_write_exe_time = (log_write_end_time - log_write_start_time).total_seconds()
        print(f"End update_log_table ({host_name},{sql_table}), total_time_taken={log_write_exe_time}")    
    except Exception as e:
        sendLog(f"An error occurred while updating the log table: {str(e)}")
        print(f"An error occurred while updating the log table: {str(e)}")


# COMMAND ----------


def isEmpty(input_str):
    if  not input_str or len(input_str.strip())==0:
        return True
    return False

print(isEmpty("   "))

# COMMAND ----------

def get_rowcount(df):
    if df is None:
        return 0
    return df.count()

# COMMAND ----------

def get_last_inserted_datetime(transformed_df,watermark_column):
    if transformed_df is None:
        return None
    return transformed_df.select(max(col(watermark_column))).first()[0]

# COMMAND ----------


def find_last_folder_with_parquet_files(base_table_path):
    """
    Recursively traverse through folders and sub-folders in the data lake container
    and return the path of the last folder that contains .parquet files.

    Parameters:
    - base_table_path (str): The base path to start traversing.

    Returns:
    - str: The path of the last folder that contains .parquet files,
           or None if no such folder is found.
    """
    last_folder_path = None
    
    # Traverse through the base path and its sub-folders
    try:
        #print(f" base_table_path = {base_table_path}")
        items = dbutils.fs.ls(base_table_path)
        for item in items:
          
            if item.isDir():
                # Recursively call the function for sub-folders
                subfolder_path = find_last_folder_with_parquet_files(item.path)
                if subfolder_path:
                    last_folder_path = subfolder_path
            elif item.path.endswith(".parquet"):
                #print(f"found parquet file {item.path}")
                # Update the last folder path if .parquet file is found
                last_folder_path = base_table_path
                return last_folder_path
    except Exception as e:
        # print(f"An error occurred while traversing the path: {str(e)}")
        return None
    return last_folder_path


# COMMAND ----------


import re

def extract_row_count(message):
    # Check if the input message is a string
    if not isinstance(message, str):
        print("Error: Input message is not a string.")
        return None  
    # Define a regular expression pattern to match the row count
    pattern = r'(\d+) rows inserted'
    match = re.search(pattern, message)
    if match:
        # Extract the row count from the matched group
        row_count = int(match.group(1))
        return row_count
    else:
        # Return None if no row count is found in the message
        return None
    

# COMMAND ----------


def get_landing_path(container, storage_account, server, database, table):
    try:
        return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{server}/{database}/{table}"
    except Exception as e:
        sendLog(f"An error occurred while generating the landing path: {str(e)}")
        return f"An error occurred while generating the landing path: {str(e)}"

# COMMAND ----------

def if_table_exists(table_path):
    try:
        dbutils.fs.ls(table_path)
    except Exception as e:
        # Handle exceptions, such as if the table path doesn't exist
        print(f"{table_path} does not exist")
        return False

    return True

# COMMAND ----------

