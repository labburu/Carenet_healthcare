# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType
tables_list = ["table1", "table_2"]


landing_table_schema  = StructType([
    StructField("load_type", StringType(), True),
    StructField("source_server", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("medallion_layer", StringType(), True),
    StructField("data_lake_path", StringType(), True)
])
landing_table_data = [("landing_incremental","bronze_nhz","landing","landing_table","landing_layer","landing_lake_paty")]

bronze_table_schema  = StructType([
    StructField("load_type", IntegerType(), True),
    StructField("source_server", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("medallion_layer", StringType(), True),
    StructField("data_lake_path", StringType(), True)
])
bronze_table_data = [(1,"bronze_nhz","bronze","bronze_table","bronze_layer","bronze_lake_paty")]

landing_df = spark.createDataFrame(data=landing_table_data, schema=landing_table_schema)
bronze_df = spark.createDataFrame(data=bronze_table_data,schema=bronze_table_schema)

landing_columns_and_types = landing_df.dtypes
columns_data_types = {}
for colum, dtype in landing_columns_and_types:
    columns_data_types[colum] = dtype

bronze_columns_and_types = bronze_df.dtypes

for colum, dtype in bronze_columns_and_types:
    if columns_data_types[colum] == dtype:
        continue
    else:
        print(f"Found different data type {dtype} for the column {colum}")

# below is without data frame. direct from tables. I think below is better and faster

landing_table_schema = spark.catalog.listColumns("landing.table_name")
bronze_table_schema =  spark.catalog.listColumns("bronze.table_name")
table_column_dict = {}
for column in landing_table_schema:
    table_column_dict[column.name] = column.dataType

for column in bronze_table_schema:
    if table_column_dict[column.name] == column.dataType:
        continue
    else:
        print(f"Found different data type {dtype} for the column {colum}")