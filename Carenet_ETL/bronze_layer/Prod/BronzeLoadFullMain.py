# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC #### This notebook reads data from the landing container and write it to bronze container daily.<br />
# MAGIC ###### Tables: AccountCodeMirror,Facility,Contracts,I3_DNIS_Info_V,rlFacilityContract<br/>
# MAGIC 1. The method run_full_process is trigger point for this notebook and it invokes process_bronze_full_load<br/>
# MAGIC 2. process_bronze_full_load<br/>
# MAGIC       a. Read config file from Configuration folder for the input table<br/>
# MAGIC       b. Read data from landing container, transform and write to bronze container (BronzeLoadHelper -> transform_and_load_bronze_full)<br/> 
# MAGIC       c. Update log table with the details for the table/server in process (HelperFunctions -> update_log_table)<br/>
# MAGIC 3. Repeat for all the tables<br/>

# COMMAND ----------


import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxx.com/")
import time
from datetime import datetime
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC
# MAGIC %run "Workspace/Users/lalita.abburu@xxxx.com/bronze_layer/BronzeLoadHelper"

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/common_utils/HelperFunctions"

# COMMAND ----------


dbutils.widgets.removeAll()

# COMMAND ----------

import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F


def process_bronze_full_load(table_name):
    print(f"processing bronze full load for the table: {table_name}")
    with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
        parameter_sets = json.load(json_file)
        # Loop over parameter sets
        for params in parameter_sets[table_name]:
            parameter_values = {}
            # Set widget values
            try:
                for key, value in params.items():
                    dbutils.widgets.text(key, str(value), key)
                    parameter_values[key] = value
                    
                # Record the start time
                notebook_start_time = datetime.now()
                # print(parameter_values)
                host_name = parameter_values['host_name']
                landing_container = parameter_values['landing_container']
                bronze_container = parameter_values['bronze_container']
                storage_account = parameter_values['storage_account']
                database_name = parameter_values['database_name']
                sql_table = parameter_values['sql_table']
                hash_key_column_list =  parameter_values['hash_key_column_list']
                
                landing_base_table_path = get_base_table_path(landing_container, storage_account, host_name, database_name, sql_table)

                bronze_load_df = transform_and_load_bronze_full(parameter_values['landing_container'], parameter_values['bronze_container'], parameter_values['storage_account'], parameter_values['hash_key_column_list'], parameter_values['host_name'], parameter_values['database_name'], parameter_values['schema_name'], parameter_values['sql_table'])

                
                # Record end time
                notebook_end_time = datetime.now()
                # Calculate execution time
                execution_time = (notebook_end_time - notebook_start_time).total_seconds()
    
                row_count = 0
                if bronze_load_df is not None:
                    row_count = bronze_load_df.count()
                
                print(f"Total record count ({table_name},{host_name}) is {row_count}")
                
                last_inserted_datetime = datetime.now()
                bronze_delta_path = get_base_table_path(parameter_values['bronze_container'],parameter_values['storage_account'],parameter_values['host_name'],parameter_values['database_name'],parameter_values['sql_table'])

                if row_count > 0:
                    update_log_table(row_count,last_inserted_datetime,bronze_delta_path, "delta",notebook_start_time, notebook_end_time,"bronze", parameter_values, execution_time, "Success")
                else:
                    update_log_table(row_count,last_inserted_datetime,bronze_delta_path, "delta",notebook_start_time, notebook_end_time, "bronze",parameter_values, 0.0, "Failed-data loading")   
                
                if bronze_load_df is not None:
                    print(f"{landing_base_table_path}")
                    dbutils.fs.rm(landing_base_table_path+"/processed",recurse=True)
                    move_files(landing_base_table_path+"/ready",landing_base_table_path+"/processed")
                    print(f"Processed successfully for host_name {host_name} and table {table_name}")
            except Exception as e:
                move_files(landing_base_table_path+"/ready",landing_base_table_path+"/failed")
                print(f"Processing failed for {host_name} and {sql_table} with error: {str(e)}")
                sendLog(f"Landing to Bronze flow: Processing failed for {host_name} and {sql_table} with error: {str(e)}")
                raise Exception(str(e))


# COMMAND ----------

#run_testing_process(process_bronze_full_load, AccountCodeMirror)
run_full_process(process_bronze_full_load)

# COMMAND ----------

spark.catalog.clearCache()