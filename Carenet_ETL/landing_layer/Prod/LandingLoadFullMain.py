# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### This notebook loads data from the onprem sql table and write it to landing container daily on last_inserted_datetime in config json file. during the process
# MAGIC ###### Tables: AccountCodeMirror,Facility,Contracts,I3_DNIS_Info_V,rlFacilityContract<br/>
# MAGIC  1. The method process_landing_full_load is trigger point for this note book and it invokes run_full_process<br/>
# MAGIC  2. process_landing_full_load (This is the main method that will be triggered daily)<br/>
# MAGIC         a. Read the config file(json) for each table located at Configuration folder<br/>
# MAGIC         b. for each table/server load data from the onprem sql table (LandingLoadHelper -> connect_and_read_from_source_full_load )<br/>
# MAGIC         c. once data is read from (b) write it to the ready folder (LandingLoadHelper -> write_to_data_lake_full_load)<br/>
# MAGIC         d. after writing (c), update the log table with the details for each table (HelperFunctions -> update_log_table)<br/>
# MAGIC 3. Repeat 2 for all the tables

# COMMAND ----------


import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxx.com/")
import time
from datetime import datetime
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/common_utils/HelperFunctions"

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/landing_layer/LandingLoadHelper"

# COMMAND ----------


dbutils.widgets.removeAll()

# COMMAND ----------

import json
from datetime import datetime, timedelta


def process_landing_full_load(table_name):
    print(f"Start process_landing_full_load, table_name={table_name}")
    try:
        with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
            parameter_sets = json.load(json_file)
            print(len(parameter_sets[table_name]))

        # Initialize a dictionary to store parameter values
        parameter_values = {}
        
        # Loop over parameter sets
        for params in parameter_sets[table_name]:
            print(f"processing for {params['host_name']}")
            # Set widget values
            for key, value in params.items():
                dbutils.widgets.text(key, str(value), key)
                parameter_values[key] = value
                
            notebook_start_time = datetime.now()

            host_name = parameter_values['host_name']
            landing_table_path = get_landing_path(parameter_values)
            folders = ["ready", "processed", "failed"]
            create_folders_if_not_exist(landing_table_path, folders)
            #dbutils.fs.mkdirs(landing_table_path)
            if landing_table_exists(f"{landing_table_path}/ready"):
                # read from onprem source and write to landing container (LandingLoadHelper)
                src_to_landing_df = ingest_and_load_into_landing(parameter_values)
            else:
                print(f"{landing_table_path} does not exists")

            row_count = get_rowcount(src_to_landing_df) #(HelperFunctions)
            #last_inserted_datetime is to update log table
            last_inserted_datetime = datetime.now()
            print(f"Total record count ({table_name},{host_name}) is {row_count}")

            #notebook execution endtime
            notebook_end_time = datetime.now()

            execution_time = (notebook_end_time - notebook_start_time).total_seconds()
            #update log table code below (HelperFunctions)
            landing_table_last_folder_path = find_last_folder_with_parquet_files(landing_table_path)
            if landing_table_last_folder_path:
                update_log_table(row_count,last_inserted_datetime,landing_table_last_folder_path, "parquet",notebook_start_time, notebook_end_time,"landing",parameter_values, execution_time, "Success")
            else:
                update_log_table(row_count,last_inserted_datetime,"Path not found", "parquet",notebook_start_time, notebook_end_time,"landing", parameter_values, 0.0, "Failed-data loading failed")
        print(f"End process_landing_full_load, table_name={table_name}, total_time_taken={execution_time}")     
    except Exception as e:
        print(f"On-prem to Landing flow: processing failed for {table_name}, error={str(e)}")
        raise Exception(str(e)) 


# COMMAND ----------


# Execution of full load process
run_full_process(process_landing_full_load)

# COMMAND ----------

