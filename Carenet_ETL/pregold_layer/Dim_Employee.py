# Databricks notebook source

storage_account_name = "carenetdemodatalake"
storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxx")

# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account_name),
    storage_account_key
)

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

# Util functions

def delta_table_exists(table_path):
    try:
        dbutils.fs.ls(table_path)
    except:
        return False
    return True

# pregold_delta_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_Employee"
# print(delta_table_exists(pregold_delta_path))

def databricksTableExists(tableName,schemaName='default'):
  return spark.sql(f"show tables in {schemaName} like '{tableName}'").count()==1

# sqlTableExists = databricksTableExists("dim_employee","carenet")
# print(sqlTableExists)

# COMMAND ----------


from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType, LongType, DateType


# Define schema
schema = StructType([
    StructField("Dim_EmployeeID", LongType(), True), 
    StructField("EmployeeID", IntegerType(), True), 
    StructField("FullEmployeeID", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("HireDate", DateType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("FullName", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("MiddleName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("EmploymentType", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("OrgLevel1Code", StringType(), True),
    StructField("OrgLevel2", StringType(), True),
    StructField("SupervisorID", IntegerType(), True),
    StructField("SupervisorName", StringType(), True),
    StructField("UserName", StringType(), True), 
    StructField("LastDayWorked", DateType(), True),
    StructField("TerminationDate", DateType(), True),
    StructField("TerminationReason", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Project", StringType(), True),
    StructField("CICUserID", StringType(), True),
    StructField("EffectiveStartDate", DateType(), True),
    StructField("EffectiveEndDate", DateType(), True),
    StructField("RowHashKey", StringType(), True),
])



# Create DataFrame
data = [
    (99990101,-1, "CARHS00","test", "CARHS", datetime(1, 1, 1, 0, 0, 0), "null", "null", "null", "null", "null", "null", "null", "null", "null", 1234, "null", "null", datetime(9999, 12, 31),datetime(1, 1, 1), "none", "NY", "null", "s000", datetime(1, 1, 1),datetime(9999, 12, 31),"a9a433af4c2cc502")
]


df = spark.createDataFrame(data, schema)

# Show DataFrame
# df.display()

dim_employee_delta_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_Employee"

if not delta_table_exists(dim_employee_delta_path):
    print("Dim Employee table does not exist, so creating one...")
    # Create a Delta table if it doesn't exist
    df.write.format("delta").mode("overwrite").partitionBy("EffectiveStartDate").save(dim_employee_delta_path)
else:
    print("Dim Employee table exists")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Get Table parameters

# COMMAND ----------


import json
from datetime import datetime,timedelta

with open(f'/Workspace/Shared/Configuration/EmployeeInformation.json', 'r') as json_file:
    parameter_sets = json.load(json_file)
    print(parameter_sets)
    for params in parameter_sets["EmployeeInformation"]:
        last_inserted_datetime = params["last_inserted_datetime"]
        pregold_container = params["pregold_container"]
        watermark_column = params["watermark_column"]
        # Convert datetime string to datetime object
        datetime_obj = datetime.strptime(last_inserted_datetime, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        # Extract date from datetime object
        date_value_to_process = datetime_obj - timedelta(days=2)
        print(date_value_to_process)


# COMMAND ----------


from pyspark.sql.functions import *

employee_silver_delta_path = "abfss://silver@carenetdemodatalake.dfs.core.windows.net/EmployeeInformation"

# date_value_to_process = "2018-1-01"
employee_information_silver_df = spark.read.format("delta").load(employee_silver_delta_path).filter(to_date(col(watermark_column)) >= date_value_to_process)

# employee_information_silver_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Hash key column

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql import Window

hash_column_list = ["EmployeeID", "FullEmployeeID","EmployeeName", "Company", "HireDate", "JobTitle", "FullName", "FirstName", "MiddleName", "LastName", "FullPartTIme", "Status", "OrgLevel1Code", "OrgLevel2", "SupervisorID", "SupervisorName","UserName", "LastDayWorked","TerminationDate", "TerminationReason", "Location", "Project", "CICUserID"]

# Concatenate all columns from 'hash_column_list'
concatenated_column = concat_ws("|", *[col(column) for column in hash_column_list])

emp_df1 = employee_information_silver_df.withColumn("RowHashKey", sha2(concatenated_column, 256))

# Set column order
column_order_list = ["RowHashKey", "EmployeeID", "FullEmployeeID", "EmployeeName", "Company", "HireDate", "JobTitle", "FullName", "FirstName", "MiddleName", "LastName", "FullPartTIme", "Status", "OrgLevel1Code", "OrgLevel2", "SupervisorID", "SupervisorName","UserName", "LastDayWorked", "TerminationDate", "TerminationReason", "Location", "Project", "CICUserID", "LoadDate"]

# Get relevant columns to process further
emp_df2 = emp_df1.select(*column_order_list).orderBy("EmployeeID","LoadDate")

# emp_df1.filter((col("EmployeeID") == 12688)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # compare current row hash key with previous hash key

# COMMAND ----------



# compare hash key column with previous row
windowSpec = Window.partitionBy("EmployeeID").orderBy(asc("EmployeeID"),asc("LoadDate"))

# Add a lag column to access the hash key of the previous row
emp_df3 = emp_df2.withColumn("PrevRowHashKey", lag("RowHashKey", 1).over(windowSpec))

# Check if the current hash key matches the previous hash key
emp_df4 = emp_df3.withColumn("HashKeyMatch", 
                                   when((col("PrevRowHashKey").isNull()) & (col("TerminationDate").isNotNull()), True)
                                   .when((col("PrevRowHashKey").isNull()) & (col("TerminationDate").isNull()), False)
                                   .when((col("RowHashKey") == col("PrevRowHashKey")), True)
                                   .otherwise(False))

emp_df5 = emp_df4.filter(col("HashKeyMatch") == False).orderBy(asc("EmployeeID"),asc("LoadDate"))

# emp_df5.filter((col("EmployeeID") == 12688)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # If TerminationDate and LastDayWorked exists in a row, stop updating the employee information further

# COMMAND ----------


# Define window specification partitioned by EmployeeID and ordered by LoadDate in ascending order
window_spec = Window.partitionBy("EmployeeID").orderBy(col("LoadDate").asc())

# Get the lagged values of TerminationDate and LastDayWorked
emp_df_with_lag = emp_df5.withColumn("prev_TerminationDate", lag("TerminationDate").over(window_spec)) \
                .withColumn("prev_LastDayWorked", lag("LastDayWorked").over(window_spec))

emp_df_with_lag1 = emp_df_with_lag.withColumn("TerminationDateMatch", 
                                   when((col("prev_TerminationDate").isNotNull()) & (col("TerminationDate").isNotNull()) & (col("prev_LastDayWorked").isNotNull()) & (col("LastDayWorked").isNotNull()), True)
                                   .otherwise(False)
                                   )

emp_df6 = emp_df_with_lag1.filter(col("TerminationDateMatch") == False).orderBy(asc("EmployeeID"),asc("LoadDate"))


# emp_df6.filter((col("EmployeeID") == 12688)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data quality check for specific columns - impute NULL values with last occurrence of column value

# COMMAND ----------


column_list = ['CICUserID', 'Username', 'Company', 'OrgLevel1Code', 'OrgLevel2']

# Replace null values with preceding non-null values for each column
for column in column_list:
    emp_df6 = emp_df6.withColumn(column, last(col(column), True).over(Window.partitionBy("EmployeeID").orderBy("LoadDate")))

# emp_df6.filter((col("EmployeeID") == 800442)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Set EffectiveStartDate and EffectiveEndDate

# COMMAND ----------


import pyspark.sql.functions as F

# Define a window specification to order by load date
windowSpec = Window.partitionBy("EmployeeID").orderBy("LoadDate")

# Calculate the next row's load date
emp_df6 = emp_df6.withColumn("NextRowLoadDate", F.lead("LoadDate").over(windowSpec))

emp_df7 = emp_df6.withColumn("EffectiveStartDate", F.col("LoadDate").cast("date"))

# Calculate effective end date
emp_df8 = emp_df7.withColumn("EffectiveEndDate", 
                   F.when(F.col("NextRowLoadDate").isNull(), F.lit("9999-12-31"))
                   .otherwise(F.date_sub(F.col("NextRowLoadDate"), 1)))

# emp_df8.filter((col("EmployeeID") == 12688)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Handle Employees with Re-Hires and set EffectiveEndDate accordingly

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, last

# Define a window partitioned by EmployeeID and ordered by Date
windowSpec = Window.partitionBy("EmployeeID").orderBy("LoadDate")

# Define the condition to check for the last occurrence of hire date, termination date, and last day worked
condition = (
    (col("HireDate").isNotNull()) &
    (col("TerminationDate").isNotNull()) &
    (col("LastDayWorked").isNotNull())
)

# Set the effective end date as '9999-12-31' for the last occurrence of the condition
emp_df8= emp_df8.withColumn("EffectiveEndDate", 
                   when(last(condition, ignorenulls=True).over(windowSpec), "9999-12-31").otherwise(col("EffectiveEndDate")))


# emp_df8.filter((col("EmployeeID") == 12688)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Handle TerminationReason and Status columns 

# COMMAND ----------

# Handle TerminationReason and Status columns 

emp_df8 = emp_df8.withColumn("TerminationReason", 
                        when((col("TerminationDate").isNull()) | (col("TerminationReason") == "<None>"), lit("null"))
                        .otherwise(col("TerminationReason"))) \
                 .withColumn("Status", 
                        when(col("TerminationDate").isNotNull(), lit("Terminated")) \
                        .otherwise(col("Status"))).cache()

# emp_df8.filter((col("EmployeeID") == 12688)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Row Identifier

# COMMAND ----------



emp_df_with_numeric_date = emp_df8.withColumn("EffectiveStartDateNumeric", 
                                       regexp_replace(col("EffectiveStartDate"), "-", "").cast("bigint"))

emp_df_with_rowid = emp_df_with_numeric_date \
                        .withColumn("Dim_EmployeeID", concat((1000000 + col("EmployeeID")), col("EffectiveStartDateNumeric")).cast("bigint")) \
                        .withColumnRenamed("FullPartTIme", "EmploymentType")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC # Modify schema for date columns

# COMMAND ----------


# Define a function to cast timestamp columns as date
def cast_timestamp_to_date(df):
    for col_name, col_type in df.dtypes:
        if col_type == "timestamp":
            df = df.withColumn(col_name, col(col_name).cast(DateType()))
        elif col_name == "EffectiveEndDate":
            df = df.withColumn(col_name, col(col_name).cast(DateType()))
    return df

# Apply the function to the DataFrame
emp_df_casted = cast_timestamp_to_date(emp_df_with_rowid)
# emp_df_casted.display()

# COMMAND ----------


# Realign Column order before merge
from pyspark.sql.functions import *
from pyspark.sql import Window

column_order_list = ["Dim_EmployeeID", "EmployeeID", "FullEmployeeID","EmployeeName", "Company", "HireDate", "JobTitle", "FullName", "FirstName", "MiddleName", "LastName", "EmploymentType", "Status", "OrgLevel1Code", "OrgLevel2", "SupervisorID", "SupervisorName","UserName", "LastDayWorked","TerminationDate", "TerminationReason", "Location", "Project", "CICUserID","EffectiveStartDate", "EffectiveEndDate", "RowHashKey"]

final_emp_for_merge_df = emp_df_casted.select(*column_order_list).orderBy("EmployeeID","EffectiveStartDate")

# final_emp_for_merge_df.filter((col("EmployeeID") == 12688)).orderBy(asc("EmployeeID"),asc("LoadDate")).display()

# COMMAND ----------


from delta.tables import *

dim_employee_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_Employee"

dim_employee_pregold_target_df = spark.read.format("delta").load(dim_employee_path)

if dim_employee_pregold_target_df.count() == 1:
    final_emp_for_merge_df.write.format("delta").mode("append").partitionBy("EffectiveStartDate").save(dim_employee_path)
else:
    dim_employee_delta_table = DeltaTable.forPath(spark, dim_employee_path)
    hash_key_column = "RowHashKey"
    # Merge the source DataFrame into the target Delta table
    dim_employee_delta_table.alias("target") \
        .merge(final_emp_for_merge_df.alias("source"), f"target.{hash_key_column} = source.{hash_key_column}") \
        .whenNotMatchedInsertAll() \
        .execute()


# COMMAND ----------


spark.sql("USE CATALOG carenet_dev")
spark.sql("create schema if not exists pregold")
spark.sql("create table if not exists pregold.dim_employee using delta location 'abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_Employee' ")

# COMMAND ----------


# Record end time
notebook_end_time = datetime.now()
# Calculate execution time
execution_time = (notebook_end_time - notebook_start_time).total_seconds()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC # Update Log Table

# COMMAND ----------


dim_emp_row_count = final_emp_for_merge_df.count()
print(last_inserted_datetime)

table_json_data = {}

table_json_data['landing_container'] = "N/A"
table_json_data['storage_account'] = "carenetdemodatalake"
table_json_data['host_name'] = "N/A"
table_json_data['database_name'] = " "
table_json_data['sql_table'] = "Dim_Employee"
table_json_data['load_type'] = "Incremental"
table_json_data['watermark_column'] = watermark_column

dim_employee_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_Employee"

if dim_emp_row_count > 0:
    update_log_table(dim_emp_row_count, last_inserted_datetime, dim_employee_path, "delta",notebook_start_time, notebook_end_time,"pregold", table_json_data, execution_time, "Success")
else:
    update_log_table(dim_emp_row_count,last_inserted_datetime, dim_employee_path, "delta",notebook_start_time, notebook_end_time, "pregold",table_json_data, 0.0, "Failed-data loading")  


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Update Last inserted datetime in Configuration file for next load

# COMMAND ----------


import json

def update_json_file(df, table_name, date_column):
    with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'r') as json_file:
                parameter_sets = json.load(json_file)
                # print(parameter_sets)
                for params in parameter_sets[table_name]:
                    max_date = df.select(max(col(date_column))).first()[0]
                    params["last_inserted_datetime"] = str(max_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
                updated_json_string = json.dumps(parameter_sets, indent=2)
                # print(updated_json_string)
                with open(f'/Workspace/Shared/Configuration/{table_name}.json', 'w') as json_file:
                    json_file.write(updated_json_string)

update_json_file(final_emp_for_merge_df, "EmployeeInformation", "EffectiveStartDate")

# COMMAND ----------


# for Testing only
# dim_employee_path = "abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_Employee"

# dim_employee_pregold_df = spark.read.format("delta").load(dim_employee_path)

# dim_employee_pregold_df.filter(col("EmployeeID") == 12688).orderBy(asc("EmployeeID"),asc("EffectiveStartDate")).display()

