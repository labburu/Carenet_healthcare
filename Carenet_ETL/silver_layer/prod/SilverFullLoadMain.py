# Databricks notebook source
# MAGIC %md
# MAGIC #### This notebook reads data from the bronze container and write it to silver container daily.<br />
# MAGIC ###### Tables: AccountCodeMirror,Facility,Contracts,I3_DNIS_Info_V,rlFacilityContract<br/>
# MAGIC 1. The method run_full_prod_process is trigger point for this notebook and it invokes process_silver_full_load<br/>
# MAGIC 2. process_silver_full_load<br/>
# MAGIC       a. Read config file from Configuration folder for the input table<br/>
# MAGIC       b. Read data from bronze container and return the dataframe(get_bronze_data)<br/> 
# MAGIC       c. Transform the bronze data that is returned  from (b) (SilverLoadHelper ->transform_bronze_data) <br/>
# MAGIC       d. write_data_to_silver_path, write the dataframe that is returned from (c) to silver container <br/>
# MAGIC       d. Update log table with the details for the table/server in process (HelperFunctions -> update_log_table)<br/>
# MAGIC       e. update the last_inserted_date_time.json file with the last_inserted_datetime (this is used as a reference to update main table config files in preparation for next load) <br/>
# MAGIC 3. Repeat for all the tables<br/>
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import libraries

# COMMAND ----------


import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxx.com/")
import time
from datetime import datetime
from common_utils.AppInsights import *



# COMMAND ----------

storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxx")
# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format('carenetdemodatalake'),
    storage_account_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Execute Helper Functions

# COMMAND ----------

# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/silver_layer/prod/SilverLoadHelper"

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ShortType, DateType, LongType, DecimalType, BooleanType
import pytz


def get_bronze_data(table_name):
    with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
        parameter_sets = json.load(json_file)
    df_list = []
    # We are establishing a 'failed_record_dict' to update the log table without altering the JSON file (last_inserted_datetime), allowing subsequent iterations to query data from the previous successful run.
    failed_record_dict = {}
    # We are establishing a 'success_record_dict' to update the log table and the JSON file (last_inserted_datetime) with latest date time.
    success_record_dict = {}
    #We are establishing a 'current_batch_last_inserted_datetime_dict' to capture last_inserted_datetime for individual server for tables (Each server will have its own time, this is to capture time for individual server)
    current_batch_last_inserted_datetime_dict = {}
    for params in parameter_sets[table_name]:
        parameter_values = {}
        try:
            for key, value in params.items():
                dbutils.widgets.text(key, str(value), key)
                parameter_values[key] = value
            silver_container = parameter_values["silver_container"]
            load_type = parameter_values["load_type"]
            bronze_container = parameter_values["bronze_container"]
            storage_account = parameter_values["storage_account"]
            host_name = parameter_values["host_name"]
            database_name = parameter_values["database_name"]
            sql_table = parameter_values["sql_table"]    
            # Record the start time
            notebook_start_time = datetime.now()              
            bronze_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/{host_name}/{database_name}/{sql_table}"
            silver_delta_path = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/{sql_table}"
            #Read from bronze path
            df = spark.read.format("delta").option("numPartitions",5).load(bronze_path)

            source_cnt = df.count()
            if source_cnt==0:
                print(f"no data found for {host_name} and {table_name}")
                #failed_record_dict[host_name] = parameter_values
            else:
                success_record_dict[host_name] = parameter_values
                df_list.append(df)
                current_batch_last_inserted_datetime = datetime.now()
                last_inserted_dattime_str = str(current_batch_last_inserted_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
                if sql_table in current_batch_last_inserted_datetime_dict:
                    current_batch_last_inserted_datetime_dict[sql_table].append([host_name,database_name,last_inserted_dattime_str])
                else:
                    current_batch_last_inserted_datetime_dict[sql_table] = [[host_name,database_name,last_inserted_dattime_str]]
                
        except Exception as e:
            failed_record_dict[host_name] = parameter_values
            print(f"Bronze to Silver flow: Error occured in get_bronze_data, table_name={table_name} error={str(e)}")
            sendLog(f"Bronze to Silver flow: Error occured in get_bronze_data, table_name={table_name} error={str(e)}")
    
    return df_list,success_record_dict,failed_record_dict,silver_delta_path,current_batch_last_inserted_datetime_dict

# COMMAND ----------

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

def process_silver_full_load(table_name):
    print(f"Start process_silver_full_load, table_name={table_name}")
    try:
        df_list = []
        failed_record_dict = {}
        success_record_dict = {}
        # Load data from bronze path
        notebook_start_time = datetime.now()        
        df_list,success_record_dict,failed_record_dict,silver_delta_path,current_batch_last_inserted_datetime_dict =  get_bronze_data(table_name)
        

        if len(df_list) == 0:
            print(f"No data found to process further for the table {table_name}")
            update_log_table_helper(0,None,{},{},silver_delta_path,notebook_start_time)
            return
        
        # transform the data received from bronze path (SilverLoadHelper->transform_bronze_data)
        transformed_df = transform_bronze_data(df_list)
        # write transformed data to silver path
        transformed_df.write.format("delta").mode("overwrite").save(silver_delta_path)
        
        #Get row_count and last_inserted_datetime(HelperFunctions -> get_rowcount)
        transformed_df_count = get_rowcount(transformed_df)
        # Get current datetime
        current_datetime = datetime.now()

        # Format datetime object as string in the specified format
        last_inserted_datetime = current_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        last_inserted_datetime_filepath = '/Workspace/Shared/Configuration/last_inserted_date_time_full.json'
        # update log table and last_inserted_date_time for individual servers in a file
        # update_log_table_helper->SilverLoadHelper
        # update_last_inserted_datetime_file -> SilverLoadHelper
        update_log_table_helper(transformed_df_count,last_inserted_datetime,success_record_dict,failed_record_dict,silver_delta_path,notebook_start_time)
        update_last_inserted_datetime_file(current_batch_last_inserted_datetime_dict,last_inserted_datetime_filepath)

        print(f"End process_silver_full_load, table_name={table_name}")
    except Exception as e:
        print(f"Bronze to Silver flow: Error occured in process_silver_full_load, table_name={table_name}, error: {str(e)}")
        sendLog(f"Bronze to Silver flow: Error occured in process_silver_full_load, table_name={table_name}, error: {str(e)}")
        

# COMMAND ----------


delete_file('/Workspace/Shared/Configuration/','last_inserted_date_time_full.json')
run_full_process(process_silver_full_load)

# COMMAND ----------


silver_table_list = ["AccountCodeMirror", "Facility", "Contracts", "I3_DNIS_Info_V", "rlFacilityContract"]

for table in silver_table_list:
    # spark.sql("CREATE CATALOG IF NOT EXISTS carenet_dev")
    spark.sql("USE CATALOG carenet_dev")
    spark.sql("create schema if not exists silver")
    spark.sql(f"create table if not exists silver.{table} using delta location 'abfss://silver@carenetdemodatalake.dfs.core.windows.net/{table}' ")