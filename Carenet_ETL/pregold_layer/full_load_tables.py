# Databricks notebook source

pregold_table_list = ["AccountCodeMirror", "Facility", "Contracts", "I3_DNIS_Info_V", "rlFacilityContract"]

for table in pregold_table_list:
    silver_delta_path = f"abfss://silver@carenetdemodatalake.dfs.core.windows.net/{table}"
    pregold_delta_path = f"abfss://pregold@carenetdemodatalake.dfs.core.windows.net/{table}"
    silver_df = spark.read.format("delta").load(silver_delta_path)
    silver_df.write.format("delta").mode("overwrite").save(pregold_delta_path)

# COMMAND ----------

pregold_table_list = ["AccountCodeMirror", "Facility", "Contracts", "I3_DNIS_Info_V", "rlFacilityContract"]

for table in pregold_table_list:
    spark.sql("USE CATALOG carenet_dev")
    spark.sql("create schema if not exists pregold")
    spark.sql(f"create table if not exists pregold.{table} using delta location 'abfss://pregold@carenetdemodatalake.dfs.core.windows.net/{table}' ")