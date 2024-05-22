# Databricks notebook source
storage_account_name = "carenetdemodatalake"
storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxxx")

# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account_name),
    storage_account_key)

# COMMAND ----------

from datetime import datetime
import sys
sys.path.append("/Workspace/Users/lalita.abburu@xxxx.com/")

# Record the start time
notebook_start_time = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Call Common Utils Library

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Users/lalita.abburu@xxxx.com/common_utils/HelperFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Custom functions

# COMMAND ----------


def delta_table_exists(table_path):
    try:
        dbutils.fs.ls(table_path)
    except:
        return False
    return True

# pregold_delta_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Fact_InteractionSummaryDetails"
# print(delta_table_exists(pregold_delta_path))

def databricksTableExists(tableName,schemaName='default'):
  return spark.sql(f"show tables in {schemaName} like '{tableName}'").count()==1

# COMMAND ----------



import json
from datetime import datetime, timedelta

def get_earliest_date(table_name):
    try:
        with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
            print(f"table_name={table_name}")
            interaction_summary_json = json.load(json_file)
        # Extract "last_inserted_datetime" values from the list
        date_values = [entry["last_inserted_datetime"] for entry in interaction_summary_json[table_name]]
        # Convert each string to a datetime object
        date_objects = [datetime.fromisoformat(date_value.replace('Z', '+00:00')) for date_value in date_values]
        sorted_date_objects = sorted(date_objects)

        return sorted_date_objects[0]
    except Exception as e:
        # Handle missing keys or invalid date formats
        return f"Error: {str(e)}"

    

is_last_inserted_datetime = get_earliest_date("InteractionSummary")
ica_last_inserted_datetime = get_earliest_date("InteractionCustomAttributes")


print(is_last_inserted_datetime)
print(ica_last_inserted_datetime)


# COMMAND ----------




def get_table_config_details(table_name):
    with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
        print(f"table_name={table_name}")
        interaction_summary_json = json.load(json_file)
        watermark_column = interaction_summary_json[table_name][0]["watermark_column"]
        silver_container = interaction_summary_json[table_name][0]["silver_container"]
        storage_account = interaction_summary_json[table_name][0]["storage_account"]

    return watermark_column, silver_container, storage_account


is_config_details = get_table_config_details("InteractionSummary")
is_watermark_column = is_config_details[0]
is_silver_container = is_config_details[1]
is_storage_account = is_config_details[2]
print(is_watermark_column)
print(is_silver_container)
print(is_storage_account)

ica_config_details = get_table_config_details("InteractionCustomAttributes")
ica_watermark_column = ica_config_details[0]
ica_silver_container = ica_config_details[1]
ica_storage_account = ica_config_details[2]
print(ica_watermark_column)
print(ica_silver_container)
print(ica_storage_account)

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter the silver delta tables for incremental loads

# COMMAND ----------

from pyspark.sql.functions import *

# is_last_inserted_datetime = "2019-12-31T23:59:59.000Z"
# ica_min_last_inserted_datetime = "2019-12-31T23:59:59.000Z"


silver_path1 = f"abfss://{is_silver_container}@{is_storage_account}.dfs.core.windows.net/InteractionSummary"
interaction_summary_df = spark.read.format("delta").load(silver_path1).filter(col(is_watermark_column)> is_last_inserted_datetime)
# interaction_summary_df.display()


silver_path2 = f"abfss://{ica_silver_container}@{ica_storage_account}.dfs.core.windows.net/InteractionCustomAttributes"
interaction_custom_attributes_df = spark.read.format("delta").load(silver_path2).filter(col(ica_watermark_column)> ica_last_inserted_datetime)


# COMMAND ----------

# MAGIC %md
# MAGIC # Join InteractionSummary and InterationCustomAttributes

# COMMAND ----------


interaction_summary_df.createOrReplaceTempView("interation_summary_view")
interaction_custom_attributes_df.createOrReplaceTempView("interaction_custom_attributes_view")


fact_interaction_summary_details_df = spark.sql(""" 
                        SELECT
                          interation_summary_view.InteractionIDKey,
                          interation_summary_view.SiteID,
                          interation_summary_view.SeqNo,
                          interation_summary_view.AccountCode,
                          interation_summary_view.Direction,
                          interation_summary_view.ConnectionType,
                          interation_summary_view.MediaType,
                          interation_summary_view.DNIS_LocalID as DNIS,
                          interation_summary_view.StartDTOffset,
                          interation_summary_view.InitiatedDateTimeUTC,
                          interation_summary_view.ConnectedDateTimeUTC,
                          interation_summary_view.TerminatedDateTimeUTC,
                          interation_summary_view.LastLocalUserId,
                          interation_summary_view.LastAssignedWorkgroupID,
                          interation_summary_view.RemoteName,
                          interation_summary_view.FirstAssignedAcdSkillSet,
                          interation_summary_view.tDialing,
                          interation_summary_view.tIVRWait,
                          interation_summary_view.tQueueWait as QueueWait,
                          interation_summary_view.tAlert,
                          interation_summary_view.tConnected,
                          interation_summary_view.tHeld,
                          interation_summary_view.tConference,
                          interation_summary_view.tExternal,
                          interation_summary_view.tACW,
                          interation_summary_view.nIVR,
                          interation_summary_view.nQueueWait,
                          interation_summary_view.nTalk,
                          interation_summary_view.nConference,
                          interation_summary_view.nHeld,
                          interation_summary_view.nTransfer,
                          interation_summary_view.nExternal,
                          interation_summary_view.__PartitionDateUTC,
                          interaction_custom_attributes_view.InteractionIDKey as callId, 
                          interaction_custom_attributes_view.CustomNum1,
                          interaction_custom_attributes_view.CustomNum2,
                          interaction_custom_attributes_view.CustomNum3, 
                          interaction_custom_attributes_view.CustomNum4, 
                          interaction_custom_attributes_view.CustomString1,
                          interaction_custom_attributes_view.CustomString2, 
                          interaction_custom_attributes_view.CustomString3, 
                          interaction_custom_attributes_view.CustomString4,
                          interation_summary_view.__record_created_datetime_utc as RecordCreatedDateTimeUTC,
                          interation_summary_view.__record_created_datetime_cst as RecordCreatedDateTimeCST
                        FROM 
                            interation_summary_view 
                        LEFT JOIN  interaction_custom_attributes_view ON 
                          interation_summary_view.InteractionIDKey = interaction_custom_attributes_view.InteractionIDKey
                          and interation_summary_view.SiteID = interaction_custom_attributes_view.SiteID
                          and interation_summary_view.SeqNo = interaction_custom_attributes_view.SeqNo
                        """)

# fact_interaction_summary_details_df.display()
    


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add HashKey and FactInteractionID column

# COMMAND ----------



column_list = ["InteractionIDKey", "SiteID", "SeqNo"]
concatenated_column = concat_ws("|", *[col(column) for column in column_list])
fact_interaction_summary_details_df = fact_interaction_summary_details_df.withColumn("InteractionHashKey", sha2(concatenated_column, 256))
fact_interaction_summary_details_df = fact_interaction_summary_details_df.withColumn("Fact_InteractionID", concat(col("InteractionIDKey"), col("SiteID"),col("SeqNo")))
column_order_list = ['Fact_InteractionID','InteractionHashKey','InteractionIDKey','SiteID','SeqNo','AccountCode','Direction','ConnectionType','MediaType','DNIS','StartDTOffset','InitiatedDateTimeUTC','ConnectedDateTimeUTC','TerminatedDateTimeUTC','LastLocalUserId','LastAssignedWorkgroupID','RemoteName','FirstAssignedAcdSkillSet','tDialing','tIVRWait','QueueWait','tAlert','tConnected','tHeld','tConference','tExternal','tACW','nIVR','nQueueWait','nTalk','nConference','nHeld','nTransfer','nExternal','callId','CustomNum1','CustomNum2','CustomNum3','CustomNum4','CustomString1','CustomString2','CustomString3','CustomString4','RecordCreatedDateTimeUTC','RecordCreatedDateTimeCST','__PartitionDateUTC']
fact_interaction_summary_details_df = fact_interaction_summary_details_df.select(*column_order_list)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Perform Delta loads

# COMMAND ----------


from delta.tables import *


pregold_ineraction_summary_details_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Fact_InteractionSummaryDetails"


if not delta_table_exists(pregold_ineraction_summary_details_path):
    fact_interaction_summary_details_df.write.format("delta").mode("overwrite").partitionBy("__PartitionDateUTC").save(pregold_ineraction_summary_details_path)
else:
    # fact_interaction_summary_details_pregold_target_df = spark.read.format("delta").load(pregold_ineraction_summary_details_path)
    fact_interaction_summary_details_pregold_delta_table = DeltaTable.forPath(spark, pregold_ineraction_summary_details_path)
    # # Define the hash key columns
    hash_key_columns = ["Fact_InteractionID", "InteractionIDKey", "SiteID", "SeqNo"]
    # # Define the merge condition based on multiple key columns
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in hash_key_columns])
    # Merge the source DataFrame into the target Delta table
    fact_interaction_summary_details_pregold_delta_table.alias("target") \
            .merge(fact_interaction_summary_details_df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC # Create table in Catalog

# COMMAND ----------


spark.sql("USE CATALOG carenet_dev")
spark.sql("create schema if not exists pregold")
sqlTableExists = databricksTableExists("fact_interactionsummarydetails","pregold") 

FactInteractionSummary_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Fact_InteractionSummaryDetails"

if not sqlTableExists:
  print("Creating table pregold.fact_interactionsummarydetails...")
  spark.sql(f""" CREATE TABLE pregold.fact_interactionsummarydetails USING DELTA LOCATION '{FactInteractionSummary_path}' """) 

# COMMAND ----------


# Record end time
notebook_end_time = datetime.now()
# Calculate execution time
execution_time = (notebook_end_time - notebook_start_time).total_seconds()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Update Log Table

# COMMAND ----------


fact_interaction_summary_details_row_count = fact_interaction_summary_details_df.count()
fact_interaction_summary_details_max_date = fact_interaction_summary_details_df.agg(max(is_watermark_column)).collect()[0][0]
print(fact_interaction_summary_details_max_date)

table_json_data = {}

table_json_data['landing_container'] = "N/A"
table_json_data['storage_account'] = "carenetdemodatalake"
table_json_data['host_name'] = "N/A"
table_json_data['database_name'] = " "
table_json_data['sql_table'] = "Fact_InteractionSummaryDetails"
table_json_data['load_type'] = "Incremental"
table_json_data['watermark_column'] = "TerminatedDateTimeUTC"

FactInteractionSummary_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Fact_InteractionSummaryDetails"

if fact_interaction_summary_details_row_count > 0:
    update_log_table(fact_interaction_summary_details_row_count,fact_interaction_summary_details_max_date,FactInteractionSummary_path, "delta",notebook_start_time, notebook_end_time,"pregold", table_json_data, execution_time, "Success")
else:
    update_log_table(fact_interaction_summary_details_row_count,fact_interaction_summary_details_max_date,FactInteractionSummary_path, "delta",notebook_start_time, notebook_end_time, "pregold",table_json_data, 0.0, "Failed-data loading")  




# COMMAND ----------


spark.catalog.clearCache()

# COMMAND ----------


# %sql

# select * from silver.fact_interactionsummary_details where year(TerminatedDateTimeUTC) = 2024 and month(TerminatedDateTimeUTC) = 3 order by TerminatedDateTimeUTC