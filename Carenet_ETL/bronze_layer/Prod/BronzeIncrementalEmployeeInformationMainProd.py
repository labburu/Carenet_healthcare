# Databricks notebook source
# MAGIC %md
# MAGIC #### This notebook reads data from the landing container and write it to bronze container daily based on last_inserted_datetime read from config json file.<br />
# MAGIC ###### Tables: EmployeeInformation<br/>
# MAGIC 1. The method run_incremental_prod_process is trigger point for this notebook and it invokes process_bronze_incremental_load<br/>
# MAGIC 2. process_bronze_incremental_load<br/>
# MAGIC       a. Read config file from Configuration folder for the input table<br/>
# MAGIC       b.  For each table/server read data from landing container, transform and write to the bronze container (BronzeLoadHelper -> transform_and_load_bronze_incremental )<br/> 
# MAGIC       c. Get the row_count and last_inserted_datetime from the dataframe returned from (b) <br/>
# MAGIC       d. Update log table with the details for the table/server in process (HelperFunctions -> update_log_table)<br/>
# MAGIC       e. Move the files to processed folder if success or failed folder on failure <br/>
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import Libraries

# COMMAND ----------


import time
from pyspark.sql.functions import *
import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxx.com/")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC # Call Bronze helper and common utility functions

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/bronze_layer/BronzeLoadHelper"

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/common_utils/HelperFunctions"

# COMMAND ----------

# clearing up widgets for new parameter sets
dbutils.widgets.removeAll()

# COMMAND ----------



def process_bronze_incremental_load(table_name):
    print(f"processing bronze incremental load for the table : {table_name}")
    with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
        parameter_sets = json.load(json_file)

        # Loop over parameter sets for processing tables from each server
        for params in parameter_sets[table_name]:
            parameter_values = {}
            # Set widget values
            try:
                for key, value in params.items():
                    dbutils.widgets.text(key, str(value), key)
                    parameter_values[key] = value
                notebook_start_time = datetime.now()
                # print(parameter_values)
                host_name = parameter_values['host_name']
                landing_container = parameter_values['landing_container']
                bronze_container = parameter_values['bronze_container']
                storage_account = parameter_values['storage_account']
                database_name = parameter_values['database_name']
                sql_table = parameter_values['sql_table']
                
                # Record the start time
                notebook_start_time = datetime.now()


                # Read data from landing container and write to bronze container (BronzeLoadHelper)
                bronze_load_df = transform_and_load_bronze_incremental(landing_container, bronze_container, storage_account,parameter_values['watermark_column'], parameter_values['hash_key_column_list'], host_name, database_name, parameter_values['schema_name'], sql_table, parameter_values['last_inserted_datetime'])
               

                # Record end time
                notebook_end_time = datetime.now()
                # Calculate execution time
                execution_time = (notebook_end_time - notebook_start_time).total_seconds()
                # get row count from current iteraction of bronze dataframe
                row_count = get_rowcount(bronze_load_df)
                last_inserted_datetime = get_last_inserted_datetime(bronze_load_df,parameter_values['watermark_column'])

                print(f"Total record count ({table_name},{host_name}) is {row_count}")
                
                #get_base_table_path (BronzeLoadHelper)
                bronze_delta_path = get_base_table_path(parameter_values['bronze_container'],parameter_values['storage_account'],parameter_values['host_name'],parameter_values['database_name'],parameter_values['sql_table'])

                if row_count > 0:
                    update_log_table(row_count,last_inserted_datetime,bronze_delta_path, "delta",notebook_start_time, notebook_end_time,"bronze", parameter_values, execution_time, "Success")
                else:
                    update_log_table(row_count,last_inserted_datetime,bronze_delta_path, "delta",notebook_start_time, notebook_end_time, "bronze",parameter_values, 0.0, "Failed-data loading")    

                if bronze_load_df is not None:
                    landing_base_table_path = get_base_table_path(landing_container, storage_account, host_name, database_name, sql_table)
                    print(f"{landing_base_table_path}")
                    dbutils.fs.rm(landing_base_table_path+"/processed",recurse=True)
                    move_files(landing_base_table_path+"/ready",landing_base_table_path+"/processed")
                    print(f"Processed successfully for host_name {host_name} and table {table_name}")
            except Exception as e:
                move_files(landing_base_table_path+"/ready",landing_base_table_path+"/failed")
                print(f"Processing failed for {host_name}  with error: {str(e)}")
                sendLog(f"Landing to Bronze flow: Processing failed for {host_name}  with error: {str(e)}")
                raise Exception(str(e))


# COMMAND ----------


process_bronze_incremental_load("EmployeeInformation")

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

