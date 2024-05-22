# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Import Libraries

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### This notebook loads data from the onprem sql table and write it to landing container at every 30 minutes based on last_inserted_datetime in config json file. 
# MAGIC ###### Tables: InteractionSummary,InteractionCustomAttributes,Custom_SegmentsParsed<br/>
# MAGIC  1. The method run_incremental_prod_process is trigger point for this note book and it invokes process_landing_incremental_load<br />
# MAGIC  2. process_landing_incremental_load (This is the main method that will be triggered at every 30 mins)<br/>
# MAGIC      a. Read the config file(json) for each table located at Configuration folder parallelly<br/>
# MAGIC      b. for each table/server load data from the onprem sql table (LandingLoadHelper -> connect_and_read_from_source_incremental_load )<br/>
# MAGIC      c. once data is read from (b) write it to the ready folder (LandingLoadHelper -> write_to_data_lake_incremental_load)<br/>
# MAGIC      d. after writing (c), update the log table with the details for each table (HelperFunctions -> update_log_table)<br/>
# MAGIC   3. Repeat 2 for all tables

# COMMAND ----------


import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxx.com/")
import time
from datetime import datetime
from pyspark.sql.functions import *
from common_utils.AppInsights import *


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Call helper functions from common utilities folder

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/common_utils/HelperFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Call Landing helper functions

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/landing_layer/LandingLoadHelper"

# COMMAND ----------


# Clean up parameter widgets for new parameter loads
dbutils.widgets.removeAll()

# COMMAND ----------

import json
from datetime import datetime, timedelta

# This method loads the data from the onprem sql table to landing container
# 1. Read table config json file
# 2. Create folders "ready", "processed", "failed" if not already created
# 3. Load data from onprem sql to landing container  (LandingLoadHelper -> ingest_and_load_into_landing)
# 4. get the row_count and last_inserted_datetime from the dataframe returned from step(3)
# 5. update log table(HelperFunctions -> update_log_table) with the details for the table/server that is in process
# 6. For any table/server that is in processing fails, log the details and continue processig for the next server

def process_landing_incremental_load(table_name):
    print(f"Start process_landing_incremental_load, table_name={table_name}")
    landing_start_time = datetime.now()
    try:
        with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
            parameter_sets = json.load(json_file)
        # Initialize a dictionary to store parameter values
        parameter_values = {}
        # Loop over parameter sets
        for params in parameter_sets[table_name]:
            try:
            # Set widget values
                for key, value in params.items():
                    dbutils.widgets.text(key, str(value), key)
                    parameter_values[key] = value
                notebook_start_time = datetime.now()
                landing_table_path = get_landing_path(parameter_values)
                host_name = parameter_values["host_name"]
                folders = ["ready", "processed", "failed"]

                create_folders_if_not_exist(landing_table_path, folders)
                row_count = 0
                if landing_table_exists(f"{landing_table_path}/ready"):
                   # read from onprem source and write to landing container (LandingLoadHelper)
                    src_to_landing_df= ingest_and_load_into_landing(parameter_values)
                else:
                    print(f" landing table does not exists")
                    continue
                
                #get row_count and last_inserted_datetime for updating log table (HelperFunctions)
                row_count = get_rowcount(src_to_landing_df)
                last_inserted_datetime = get_last_inserted_datetime(src_to_landing_df,parameter_values['watermark_column'])
                
                print(f"Total record count ({table_name},{host_name}) is {row_count}")

                # Record end time
                notebook_end_time = datetime.now()
                # Calculate execution time
                execution_time = (notebook_end_time - notebook_start_time).total_seconds()
                
                landing_table_last_folder_path = find_last_folder_with_parquet_files(landing_table_path)
                if landing_table_last_folder_path:
                    update_log_table(row_count,last_inserted_datetime,landing_table_last_folder_path, "parquet",notebook_start_time, notebook_end_time,"landing",parameter_values, execution_time, "Success")
                else:
                    update_log_table(row_count,last_inserted_datetime,"Path not found", "parquet",notebook_start_time, notebook_end_time,"landing", parameter_values, 0.0, "Failed-data loading failed")

            except Exception as ex:
                #this exception block handles exception for the individual table/server and lets continue for other table/server
                sendLog(str(ex))
                print(str(ex))
        landing_end_time = datetime.now()
        landing_execution_time = (landing_end_time - landing_start_time).total_seconds()
        print(f"End process_landing_incremental_load, table_name={table_name}, total_time_taken={landing_execution_time}")
    except Exception as e:
        print(f"On-prem to landing process: Exception occured in process_landing_incremental_load, table_name={table_name}, error={str(e)}")
        sendLog(f"On-prem to landing process: Exception occured in process_landing_incremental_load, table_name={table_name}, error={str(e)}")
        raise Exception(str(e))


# COMMAND ----------


# This is the trigger point for this notebook
# run_incremental_prod_process(process_landing_incremental_load)

# table_list = ["InteractionSummary","InteractionCustomAttributes"]

for table in get_tables_prod_list():
    process_landing_incremental_load(table)


# COMMAND ----------

# After running the notebook clear cache 
spark.catalog.clearCache()

# COMMAND ----------

