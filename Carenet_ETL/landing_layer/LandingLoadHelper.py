# Databricks notebook source
# This a helper note book created for code reusability for landing process for both incremental/full

# COMMAND ----------

storage_account = "carenetdemodatalake"
storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxxxxxx")


# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account),
    storage_account_key
)


# COMMAND ----------


import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxxxx.com/")

# COMMAND ----------

# This method takes table_json_data that is loaded from config (json) file for incremental process
# Get the required details to connect to the onprem sql server
# Construct sql query with the table and last_inserted_datetime that is retrieved from the config (json) file
# Query the onprem sql server with the constructed query
# return the dataframe


from databricks.sdk.runtime import *
from common_utils.AppInsights import *
from datetime import datetime

# Read data from on-prem incremental
def connect_and_read_from_source_incremental_load(table_json_data):
    """
    Connect to a SQL Server database and return a Spark DataFrame.
    
    Parameters:
    - server (str): SQL Server hostname or IP address
    - database (str): Name of the database
    - username (str): Database username
    - password (str): Database password
    
    Returns:
    - DataFrame: Spark DataFrame connected to the SQL Server database
    """
    try:
        host_name = table_json_data['host_name']
        host_secret = table_json_data['host_secret']
        db_username_secret = table_json_data['db_username_secret'] 
        db_password_secret = table_json_data['db_password_secret'] 
        schema = table_json_data['schema_name']
        table = table_json_data['sql_table']
        watermark_column = table_json_data['watermark_column']
        database = table_json_data['database_name']
        last_inserted_datetime = table_json_data['last_inserted_datetime']
        # last_inserted_datetime = subtract_30_minutes_from_now()#THIS NEED TO BE REMOVED AFTER TESTING
        table_query = f"(select * from {schema}.{table} WHERE {watermark_column} > '{last_inserted_datetime}') as result"
        print(f" table_query = {table_query}")

        server = dbutils.secrets.get("CarenetADLSScope",host_secret)
        username = dbutils.secrets.get("CarenetADLSScope",db_username_secret)
        password = dbutils.secrets.get("CarenetADLSScope",db_password_secret)
        port = 1433

        jdbc_url = f"jdbc:sqlserver://{server}:{port};database={database};trustServerCertificate=true;integratedSecurity=false;authenticationScheme=NTLM"

        # Define connection properties
        connection_properties = {
            "user": username,
            "password": password,
        }

        # Read data from SQL Server into a PySpark DataFrame
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", username) \
            .option("password", password) \
            .option("dbtable",table_query) \
            .load()
            
        return df
    except Exception as e:
        sendLog(f"An error occurred while reading data from source: {str(e)}")
        print(f"An error occurred while reading data from source: {str(e)}")
        return None


# COMMAND ----------


# This method takes table_json_data that is loaded from config (json) file for full process
# Get the required details to connect to the onprem sql server
# return the dataframe
def connect_and_read_from_source_full_load(table_json_data):
    """
    Connect to a SQL Server database and return a Spark DataFrame.
    
    Parameters:
    - server (str): SQL Server hostname or IP address
    - database (str): Name of the database
    - username (str): Database username
    - password (str): Database password
    
    Returns:
    - DataFrame: Spark DataFrame connected to the SQL Server database
    """
    try:
        host_name = table_json_data['host_name']
        host_secret = table_json_data['host_secret']
        db_username_secret = table_json_data['db_username_secret'] 
        db_password_secret = table_json_data['db_password_secret'] 
        schema = table_json_data['schema_name']
        table = table_json_data['sql_table']
        watermark_column = table_json_data['watermark_column']
        database = table_json_data['database_name']
         

        server = dbutils.secrets.get("CarenetADLSScope",host_secret)
        username = dbutils.secrets.get("CarenetADLSScope",db_username_secret)
        password = dbutils.secrets.get("CarenetADLSScope",db_password_secret)
        port = 1433

        jdbc_url = f"jdbc:sqlserver://{server}:{port};database={database};trustServerCertificate=true;integratedSecurity=false;authenticationScheme=NTLM"

        # Define connection properties
        connection_properties = {
            "user": username,
            "password": password,
        }

        # Read data from SQL Server into a PySpark DataFrame
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", username) \
            .option("password", password) \
            .option("dbtable", schema+"."+table) \
            .load()
            
        return df
    except Exception as e:
        sendLog(f"An error occurred while reading data from source: {str(e)}")
        print(f"An error occurred while reading data from source: {str(e)}")
        return None


# COMMAND ----------

# Write data to landing container path for incremental loads in parquet format
def write_to_data_lake_incremental_load(source_df, landing_table_path, table, host_name):
    print(f"Start write_to_data_lake_incremental_load method, table={table}")
    file_path_for_load = f"{landing_table_path}/ready"
    try:
        if source_df is None:
            return None
        end_datetime = get_end_datetime()
        file_name = f"{table}_{end_datetime}"
        write_startime = datetime.now()
        source_df.write.format("parquet").mode("overwrite").save(f"{file_path_for_load}/{file_name}")
        print(f"incremental job successfully loaded for the {table} in landing layer ") 
        write_endtime = datetime.now()
        write_executiontime = (write_endtime - write_startime).total_seconds()
        print(f"End write_to_data_lake_incremental_load method, ({table},{host_name}), total_time_taken={write_executiontime}")
        return source_df
    except Exception as e:
        sendLog(f"An error occurred while loading data in landing area: {str(e)}")
        raise Exception(str(e))
    

# Write data to landing container path for incremental loads in parquet format
def write_to_data_lake_full_load(source_df, landing_table_path, table,host_name):
    print(f"Start write_to_data_lake_full_load, table_name={table}")
    print(f"landing_table_path={landing_table_path}")
    file_path_for_load = f"{landing_table_path}/ready"
    try:
        if source_df is None:
            return None
        end_datetime = get_end_datetime()
        file_name = f"{table}_{end_datetime}"
        write_startime = datetime.now()
        # Write DataFrame to data Lake in parquet format
        source_df.write.format("parquet").mode("overwrite").save(f"{file_path_for_load}/{file_name}")
        write_endtime = datetime.now()
        write_executiontime = (write_endtime - write_startime).total_seconds()
        print(f"End write_to_data_lake_full_load, ({table},{host_name}),total_time_taken={write_executiontime}")
        return source_df
    except Exception as e:
        sendLog(f"An error occurred while loading data in landing area: {str(e)}")
        print(str(e))
        raise Exception(str(e))


# COMMAND ----------

# This method takes json_data for the table as input
# based on the load_type it calls the method to read from onprem table and write to landing container
# returns the data frame received from above step
import json
from pyspark.sql.functions import *

# Function to load data into landing container
def ingest_and_load_into_landing(table_json_data):
    try:
        load_type = table_json_data["load_type"]
        host_name = table_json_data["host_name"]
        landing_table_path = get_landing_path(table_json_data)
        sql_table =  table_json_data["sql_table"]
        
        if load_type == "full":
            source_df = connect_and_read_from_source_full_load(table_json_data)
            landing_df = write_to_data_lake_full_load(source_df, landing_table_path, sql_table, host_name)
            return landing_df
        # for incremental load type
        else:
            source_df = connect_and_read_from_source_incremental_load(table_json_data)
            landing_df = write_to_data_lake_incremental_load(source_df, landing_table_path, sql_table, host_name)
            return landing_df
    except Exception as e:
        sendLog(f"An error occurred while loading data into landing container: {str(e)}")
        print(f"An error occurred while loading data into landing container: {str(e)}")
        raise Exception(str(e))

# COMMAND ----------


# This method is only for testing. can be ignored or delted
from datetime import datetime, timedelta

def subtract_30_minutes_from_now():
    # Get the current timestamp
    current_timestamp = datetime.utcnow()
    # Subtract 30 minutes from the current timestamp
    new_timestamp = current_timestamp - timedelta(minutes=30)
    # Format the new timestamp as a string
    new_timestamp_str = new_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    return new_timestamp_str

# Test the function
# new_timestamp_str = subtract_30_minutes_from_now()
# print("New Timestamp (30 minutes earlier than current time):", new_timestamp_str)


# COMMAND ----------


# Get landing data lake path for loading
def get_landing_path(table_json_data):
    try:
        database = table_json_data['database_name']
        table = table_json_data['sql_table'] 
        container = table_json_data['landing_container'] 
        storage_account = table_json_data['storage_account'] 
        server = table_json_data['host_name'] 

        return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{server}/{database}/{table}"
    except Exception as e:
        sendLog(f"An error occurred while generating the landing path: {str(e)}")
        return f"An error occurred while generating the landing path: {str(e)}"


# Check if data lake path exists
def landing_table_exists(landing_data_path):
    try:
        dbutils.fs.ls(landing_data_path)
    except:
        return False
    return True


#Base function for creation of folders
def create_folders_if_not_exist(table_path, folders):
    """
    Create folders in the data lake container if they don't exist.

    Parameters:
    - table_path (str): table path of the data lake container.
    - folders (list): List of folder names to be created.

    """
    try:
        for folder in folders:
            folder_path = f"{table_path}/{folder}"
            dbutils.fs.mkdirs(folder_path)
            #print(f"Folder '{folder}' created successfully at '{folder_path}'")
    except Exception as e:
        return f"An error occurred while creating folders: {str(e)}"
        sendLog(f"An error occurred while creating folders: {str(e)}")


# table_path = "abfss://staging@carenetdemodatalake.dfs.core.windows.net/nhz-db-cic-dev/I3_IC/InteractionSummary"
# folders = ["ready", "processed", "failed"]
# create_folders_if_not_exist(base_path, folders)

# get current system timestamp to ad to file name in landing ready path
def get_end_datetime():
    end_datetime = datetime.now()
    end_datetime = end_datetime.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return end_datetime




# check if parquet files exist in specific dierctory path
def check_files_exist(folder_path):
    """
    Check if files exist within the specified data Lake folder.

    Parameters:
    - folder_path (str): The path to the data Lake folder.

    Returns:
    - bool: True if files exist, False otherwise.
    """
    # Check if files exist within the folder
    file_list = dbutils.fs.ls(folder_path)
    
    return len(file_list) > 0

# COMMAND ----------

