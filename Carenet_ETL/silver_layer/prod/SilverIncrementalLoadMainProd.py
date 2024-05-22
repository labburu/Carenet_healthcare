# Databricks notebook source
# MAGIC %md
# MAGIC #### This notebook reads data from the bronze container and write it to silver container at every 30 minutes based on last_inserted_datetime read from config json file.<br />
# MAGIC ###### Tables: InteractionSummary,InteractionCustomAttributes,Custom_SegmentsParsed <br/>
# MAGIC 1. The method run_incremental_prod_process is trigger point for this notebook and it invokes process_silver_incremental_load<br/>
# MAGIC 2. process_silver_incremental_load<br/>
# MAGIC       a. Read config file from Configuration folder for the input table<br/>
# MAGIC       b. Read data from bronze container and return the dataframe(get_bronze_data)<br/> 
# MAGIC       c. Transform(appends data from all servers for a specific table) the bronze data that is returned  from (b) (SilverLoadHelper ->transform_bronze_data) <br/>
# MAGIC       d. Write the dataframe that is returned from (c) to silver container(write_data_to_silver_path) <br/>
# MAGIC       d. Update log table with the details for the table/server in process (HelperFunctions -> update_log_table)<br/>
# MAGIC       e. update the last_inserted_date_time.json file with the last_inserted_datetime (this is used as a reference to update main table config files in preparation for next incremental load) <br/>
# MAGIC 3. Repeat (2) for all the tables<br/>
# MAGIC
# MAGIC  

# COMMAND ----------


import time
from datetime import datetime
import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxx.com/")
from common_utils.AppInsights import *



# COMMAND ----------


storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxx")

# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format('carenetdemodatalake'),
    storage_account_key)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/silver_layer/prod/SilverLoadHelper"

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ShortType, DateType, LongType, DecimalType, BooleanType
import pytz

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

def get_bronze_data(table_name):
    with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
        parameter_sets = json.load(json_file)
    df_list = []
    # We are establishing a 'failed_record_dict' to update the log table 
    failed_record_dict = {}
    # We are establishing a 'success_record_dict' to update the log table 
    success_record_dict = {}
    # We are establishing a 'current_batch_last_inserted_datetime_dict' to capture last_inserted_datetime for individual server for tables (Each server will have its own time, this is to capture time for individual server)
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
            watermark_column = parameter_values["watermark_column"]
            # Record the start time
            notebook_start_time = datetime.now()
            last_inserted_date_time = parameter_values["last_inserted_datetime"]
            print(f"({host_name}, {sql_table}) last_inserted_datetime = {last_inserted_date_time}")       
            bronze_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/{host_name}/{database_name}/{sql_table}"
            silver_delta_path = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/{sql_table}"
            df_filtered = spark.read.format("delta").option("numPartitions",5).load(bronze_path).filter(col(watermark_column) > last_inserted_date_time)

            source_cnt = df_filtered.count()
            if source_cnt==0:
                print(f"no data found for {host_name} and {table_name}")
            else:
                success_record_dict[host_name] = parameter_values
                df_list.append(df_filtered)
                current_batch_last_inserted_datetime = get_last_inserted_datetime(df_filtered, watermark_column)
                last_inserted_dattime_str = str(current_batch_last_inserted_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
                if sql_table in current_batch_last_inserted_datetime_dict:
                    current_batch_last_inserted_datetime_dict[sql_table].append([host_name,database_name,last_inserted_dattime_str])
                else:
                    current_batch_last_inserted_datetime_dict[sql_table] = [[host_name,database_name,last_inserted_dattime_str]]
                
        except Exception as e:
            failed_record_dict[host_name] = parameter_values
            print(f"Bronze to Silver flow: Error occured in get_bronze_data, table_name={table_name} error={str(e)}")
    
    return df_list,success_record_dict,failed_record_dict,silver_delta_path,watermark_column,current_batch_last_inserted_datetime_dict

# COMMAND ----------

#This function saves the input DataFrame to the silver delta path. It performs a merge operation to combine the incoming DataFrame with the existing delta table, ensuring no duplicate entries during the merge process.

from delta.tables import DeltaTable
def write_data_to_silver_path(transformed_df, silver_delta_path):
    print(f"Start write_data_to_silver_path, table_path={silver_delta_path}")
    try:
        silver_write_start_time = datetime.now()
        transformed_df_count = transformed_df.count()
        if transformed_df_count >0:
            if not if_table_exists(silver_delta_path):
                transformed_df.write.format("delta").mode("overwrite").partitionBy('__PartitionDateUTC').save(silver_delta_path)
                print(f"delta df count = {transformed_df_count}")
            else:
                delta_table = DeltaTable.forPath(spark, silver_delta_path)
                #broadcast_df = broadcast(transformed_df)
                delta_table.alias("target") \
                    .merge(transformed_df.alias("source"), "target.__hash_key = source.__hash_key") \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()        
        
        silver_write_end_time = datetime.now()
        silver_write_exe_time = (silver_write_end_time - silver_write_start_time).total_seconds()
        print(f"write_data_to_silver_path, row_count={transformed_df_count}, total_time_taken={silver_write_exe_time}")
    except Exception as e:
        print(f"Bronze to Silver flow:: Error occured in write_data_to_silver_path {str(e)}")



# COMMAND ----------


import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

def process_silver_incremental_load(table_name):
    print(f"Start process_silver_incremental_load_new, table_name={table_name}")
    try:
        df_list = []
        failed_record_dict = {}
        success_record_dict = {}
        notebook_start_time = datetime.now()   

        # Load data from bronze path     
        df_list,success_record_dict,failed_record_dict,silver_delta_path,watermark_column,current_batch_last_inserted_datetime_dict = get_bronze_data(table_name)
        
        if len(df_list) == 0:
            print(f"No data found to process further for the table {table_name}")
            update_log_table_helper(0,None,{},{},silver_delta_path,notebook_start_time)
            return
    
        # transform the data received from bronze path (SilverLoadHelper ->transform_bronze_data)
        transformed_df = transform_bronze_data(df_list)
        transformed_df.cache()
       
        # write transformed data to silver path
        # get_rowcount -> HelperFunctions
        # get_last_inserted_datetime -> HelperFunctions
        write_data_to_silver_path(transformed_df, silver_delta_path)
        transformed_df_count = get_rowcount(transformed_df)
        last_inserted_datetime = get_last_inserted_datetime(transformed_df,watermark_column)
        
        print(f"Total record count ({table_name}) is {transformed_df_count}")
        
        last_inserted_datetime_filepath = '/Workspace/Shared/Configuration/last_inserted_date_time.json'
        # update log table and last_inserted_date_time for individual servers in a file
        # update_log_table_helper->SilverLoadHelper
        # update_last_inserted_datetime_file -> SilverLoadHelper
        update_log_table_helper(transformed_df_count,last_inserted_datetime,success_record_dict,failed_record_dict,silver_delta_path,notebook_start_time)
        update_last_inserted_datetime_file(current_batch_last_inserted_datetime_dict,last_inserted_datetime_filepath)

        print(f"End process_silver_incremental_load_new, table_name={table_name}")
    except Exception as e:
        print(f"Bronze to Silver flow: Error occured in process_silver_incremental_load_new, table_name={table_name}, error: {str(e)}")
        sendLog(f"Bronze to Silver flow: Error occured in process_silver_full_load, table_name={table_name}, error: {str(e)}")
    finally:
        print(f"clearing cache for {table_name}")
        transformed_df.unpersist()

# COMMAND ----------


# temporary json file that captures last inserted datetime for a specific table from each of the servers, this will be used after gold layer to updated configuration file for next incremental load
# run_incremental_prod_process(process_silver_incremental_load)

delete_file('/Workspace/Shared/Configuration/','last_inserted_date_time.json')

for table in get_tables_prod_list():
    process_silver_incremental_load(table)

# COMMAND ----------


silver_table_list = ["InteractionSummary", "InteractionCustomAttributes", "Custom_SegmentsParsed"]

for table in silver_table_list:
    # spark.sql("CREATE CATALOG IF NOT EXISTS carenet_dev")
    spark.sql("USE CATALOG carenet_dev")
    spark.sql("create schema if not exists silver")
    spark.sql(f"create table if not exists silver.{table} using delta location 'abfss://silver@carenetdemodatalake.dfs.core.windows.net/{table}' ")