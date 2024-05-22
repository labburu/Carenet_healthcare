# Databricks notebook source
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/common_utils/HelperFunctions"

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ShortType, DateType, LongType, DecimalType, BooleanType
import pytz

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------


# Retrieve all column names from multiple data frames across various servers for a given table, ensuring elimination of duplicate entries.
def get_unique_cols(input_df_list):
    unique_cols_list = []
    for df in input_df_list:
        for i_col in df.columns:
            if i_col not in unique_cols_list:
                unique_cols_list.append(i_col) 
    return unique_cols_list

# COMMAND ----------

def transform_bronze_data(df_list):
    try:
        df_list = sorted(df_list, key=lambda df:len(df.columns), reverse=True)  
        # get unique columns from all the df
        unique_cols_list = get_unique_cols(df_list)
       
        #below we are working with set, so converting list into set
        all_columns_set = set(unique_cols_list)
       
        #Since there may be variations in schema for the same table from different servers, the following ensures uniformity across all tables by adding missing columns and assigning null values to them.
        sorted_df_transformed_list = []
        for df_n in df_list:
            for col_name in all_columns_set - set(df_n.columns):
                df_n = df_n.withColumn(col_name, lit(None))
            sorted_df_transformed_list.append(df_n)

        #Ensure each data frames has the same column order
        sorted_df_transformed_list = [df_n2.select(list(all_columns_set)) for df_n2 in sorted_df_transformed_list]
        if len(sorted_df_transformed_list) == 0:
            return

        # append data from different servers into one dataframe
        final_df = sorted_df_transformed_list[0]
        for transformed_sorted_df in sorted_df_transformed_list[1:]:
            final_df = final_df.union(transformed_sorted_df)

        #arrange final df in desired column order
        transformed_df = final_df.select(sorted(final_df.columns, key=lambda col: list(unique_cols_list).index(col))) 
        return transformed_df
    except Exception as e:
        print(f"Error occured in transform_bronze_data, error={str(e)}")

# COMMAND ----------

# This function updates log table for successful or failed incremental loads in the silver container. Within the silver container, only one record is written for each table, consolidating the transformed dataframe count across all servers.

#host_name, json_data = next(iter(success_record_dict.items())), We only require the json_data for updating the log table. Therefore, we extract the first record from the dictionary since the json data(except host_name and database_name) is identical across all three servers

#We are cloning the json_data because we consolidate the results for all three servers in the silver environment. Therefore, we set the host_name and database_name fields to null before transmitting the data to update the log table, so that the actual copy will remain as is for further use if any

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

def update_log_table_helper(transformed_df_count,last_inserted_datetime,success_record_dict,failed_record_dict,silver_delta_path,notebook_start_time):
    print(f"Start update_log_table_helper")
    try:
        if len(success_record_dict) > 0:
            notebook_end_time = datetime.now()
            execution_time = (notebook_end_time-notebook_start_time).total_seconds()
            host_name, json_data = next(iter(success_record_dict.items()))
            cloned_json_data = json_data.copy()
            cloned_json_data['host_name'] = None
            cloned_json_data['database_name'] = None
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                executor.submit(update_log_table,transformed_df_count,last_inserted_datetime,silver_delta_path, "delta",notebook_start_time, notebook_end_time, "silver",cloned_json_data, execution_time, "Success")

                    
        if len(failed_record_dict) > 0:
            notebook_end_time = datetime.now()
            execution_time = (notebook_end_time-notebook_start_time).total_seconds()
            host_name, json_data = next(iter(failed_record_dict.items()))
            cloned_json_data = json_data.copy()
            cloned_json_data['host_name'] = None
            cloned_json_data['database_name'] = None
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                #failed_futures_list = []
                executor.submit(update_log_table,transformed_df_count,last_inserted_datetime,silver_delta_path, "delta",notebook_start_time, notebook_end_time, "silver",cloned_json_data, execution_time, "Failed")
                   
             
        print(f"End update_log_table_helper")
    except Exception as e:
        print(f"Error occured in update_log_table_helper, error={str(e)}")



# COMMAND ----------

# creates a temp json file that stores last inserted datetime after data load in bronze layer from each server, which is used to update the main configuration file

import json

def update_last_inserted_datetime_file(current_batch_last_inserted_datetime_dict,file_path):
    print("Start update_last_inserted_datetime_file")
    try:
        if len(current_batch_last_inserted_datetime_dict) == 0:
            return
        
        if check_file_path_exists(file_path):
            with open(file_path, "r") as file:
                existing_data = json.load(file)
            existing_data.update(current_batch_last_inserted_datetime_dict)
            json_string = json.dumps(existing_data)
        else:
            json_string = json.dumps(current_batch_last_inserted_datetime_dict)
        
        with open(file_path, 'w') as json_file:
                json_file.write(json_string)
    except Exception as e:
        print(str(e))
    print("End update_last_inserted_datetime_file")

# COMMAND ----------

# Used to delete temp json file created in silver notebook that stores last inserted datetime for a specific table from each of the server

import os

def delete_file(directory_path, file_name):
    files = []
    for root, _, filenames in os.walk(directory_path):
        for filename in filenames:
            files.append(os.path.join(root, filename))

    # Print the list of files
    for file in files:
        if file.endswith(file_name):
            os.remove(file)
            break

# COMMAND ----------

# Check if dbfs file exists
import os
def check_file_path_exists(file_path):
    try:
        if os.path.exists(file_path):
            return True
    except Exception as e:
        # If an error occurs (e.g., FileNotFoundError), return False
        #print(str(e))
        return False
    return False