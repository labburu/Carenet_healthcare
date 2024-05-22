# Databricks notebook source
storage_account = "carenetdemodatalake"
storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxxxx")


# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account),
    storage_account_key
)

# COMMAND ----------


import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxxx.com/")

# COMMAND ----------


from pyspark.sql.functions import *
from common_utils.AppInsights import *
from delta.tables import DeltaTable

def get_base_table_path(container, storage_account, server, database, table):
    try:
        return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{server}/{database}/{table}/"
    except Exception as e:
        sendLog(f"An error occurred: {str(e)}")
        return None


def move_files(source_path, destination_path):
    try:
        # List contents of the source directory
        dir_contents = dbutils.fs.ls(source_path)

        # Move each folder to the destination path
        for item in dir_contents:
            if item.isDir():
                folder_name = item.name
                source_folder_path = item.path
                destination_folder_path = f"{destination_path}/{folder_name}"
                dbutils.fs.mv(source_folder_path, destination_folder_path, recurse=True)
        return f"Moved files from {source_path} to {destination_path} successfully."
    except Exception as e:
        return f"Error occurred while moving files: {str(e)}"


def bronze_table_exists(bronze_path):
    try:
        dbutils.fs.ls(bronze_path)
    except:
        return False
    return True
        


# COMMAND ----------

def transform_and_load_bronze_incremental(landing_container, bronze_container, storage_account, watermark_column, hash_key_column_list, host_name, database_name, schema_name, sql_table, last_inserted_datetime):
    try: 
        landing_base_table_path = get_base_table_path(landing_container, storage_account, host_name, database_name, sql_table)
        # gets the path from ready folder which has only the latest files to be processed
        landing_table_path = find_last_folder_with_parquet_files(landing_base_table_path+"/ready")
        bronze_delta_path = get_base_table_path(bronze_container,storage_account,host_name,database_name,sql_table)
        
        if landing_table_path is None:
            print(f"No files found in {landing_table_path}")
            return None

        #source_df= spark.read.format("parquet").option("numPartitions",10).load(landing_table_path)
        source_df= spark.read.format("parquet").load(landing_table_path)
        transformed_df = source_df \
            .withColumn("__PartitionDateUTC", to_date(col(watermark_column))) \
            .withColumn("__hash_key", sha2(concat_ws(" ", *[col(column) for column in hash_key_column_list],lit(host_name)), 256)) \
            .withColumn('__record_created_datetime_utc', to_timestamp(lit(current_timestamp()), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn('__record_created_datetime_cst', to_timestamp(lit(from_utc_timestamp(current_timestamp(),"CST")),"yyyy-MM-dd HH:mm:ss")) \
            .withColumn('__source_server', lit(host_name)) \
            .withColumn('__source_database', lit(database_name)) \
            .withColumn('__source_table', lit(schema_name+'.'+sql_table))
        #Writing to bronze delta path
        if not if_table_exists(bronze_delta_path):
            transformed_df.write.format("delta").mode("overwrite").partitionBy('__PartitionDateUTC').save(bronze_delta_path)
        else:
            delta_table = DeltaTable.forPath(spark, bronze_delta_path)
            delta_table.alias("target") \
                .merge(transformed_df.alias("source"), "target.__hash_key = source.__hash_key") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        return transformed_df      
    except Exception as e:
        move_files(landing_base_table_path+"/ready",landing_base_table_path+"/failed")
        print(f"Processing failed for the table {sql_table} and the host_name {host_name} with error: {str(e)}")
        sendLog(f"Processing failed for the table {sql_table} and the host_name {host_name} with error: {str(e)}")
        return None

# COMMAND ----------

def transform_and_load_bronze_full(landing_container, bronze_container, storage_account, hash_key_column_list, host_name, database_name, schema_name, sql_table):
    try:
        # landing_table_path = get_base_table_path(landing_container, storage_account, host_name, database_name, sql_table)
        landing_base_table_path = get_base_table_path(landing_container, storage_account, host_name, database_name, sql_table)
        # gets the path from ready folder which has only the latest files to be processed
        landing_table_path = find_last_folder_with_parquet_files(landing_base_table_path+"/ready")
        if landing_table_path is None:
            print(f"No files found in {landing_table_path}")
            return None
        bronze_delta_path = get_base_table_path(bronze_container,storage_account,host_name,database_name,sql_table)
        source_df= spark.read.format("parquet").load(landing_table_path)
        transformed_df = source_df \
            .withColumn("__hash_key", sha2(concat_ws(" ", *[col(column) for column in hash_key_column_list],lit(host_name)), 256)) \
            .withColumn('__record_created_datetime_utc', to_timestamp(lit(current_timestamp()), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn('__record_created_datetime_cst', to_timestamp(lit(from_utc_timestamp(current_timestamp(),"CST")),"yyyy-MM-dd HH:mm:ss")) \
            .withColumn('__source_server', lit(host_name)) \
            .withColumn('__source_database', lit(database_name)) \
            .withColumn('__source_table', lit(schema_name+'.'+sql_table))
        transformed_df.write.format("delta").mode("overwrite").save(bronze_delta_path)

        return transformed_df      
    except Exception as e:
        print(f" LandingToBronzeLoad notebook - 'transform_and_load_bronze_full' - Processing failed for the table {sql_table} and the host_name {host_name} with error: {str(e)}")
        sendLog(f" LandingToBronzeLoad notebook - 'transform_and_load_bronze_full' - Processing failed for the table {sql_table} and the host_name {host_name} with error: {str(e)}")
        raise Exception(str(e))
        return None