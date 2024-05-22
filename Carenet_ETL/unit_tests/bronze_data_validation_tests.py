# Databricks notebook source
storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxx")
# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format('carenetdemodatalake'),
    storage_account_key)

# COMMAND ----------

import unittest
from pyspark.sql.functions import col,count,when


def read_table(storage, container, dataset_name, table,file_type):
  path = f'abfss://{container}@{storage}.dfs.core.windows.net/{dataset_name}/{table}'   
  if file_type=="parquet":
    df = spark.read.format("parquet").load(path)
  else:
    df = spark.read.format("delta").load(path)
      # print(path)
  return df   

class Testing(unittest.TestCase):


  def setUp(self):
    self.storage = "carenetdemodatalake"
    self.container = "bronze"
    self.dataset_name = ["xxxx/Reporting","xxxx/I3_IC"]
    self.table = ["EmployeeInformation/","AccountCodeMirror/"]
    self.file_type = "delta"   
    self.primary_keys = ["EmployeeID","accountcode"]

    self.landing_storage = "carenetdemodatalake"
    self.landing_container = "landing"
    self.landing_dataset_name = ["xxxx/Reporting","xxxx/I3_IC"]
    self.landing_table = ["EmployeeInformation/processed/EmployeeInformation_2024-03-18T05:13:45.000Z","AccountCodeMirror/processed/AccountCodeMirror_2024-03-18T05:13:45.000Z"]
    self.landing_file_type = "parquet"   

  def test_count_data(self): 

    actual_rows = []
    expected_rows = []
    for dataset,table_name,land_dataset,land_table in zip(self.dataset_name,self.table,self.landing_dataset_name,self.landing_table):
    # print(dataset)
      target_df = read_table(self.storage, self.container, dataset,table_name,self.file_type)
      landing_df = read_table(self.landing_storage, self.landing_container, land_dataset, land_table,self.landing_file_type)
      new_actual_rows = target_df.count()
      new_expected_rows = landing_df.count()   
     
      actual_rows.append(new_actual_rows)      
      expected_rows.append(new_expected_rows)    
     
    for expected,actual in zip(expected_rows,actual_rows):
      # print(expected,actual)
      self.assertEqual(expected, actual, f'Expected {expected}, but got {actual}')

  def test_check_schemas(self):
    landing_schema = []
    target_schema = []
    for dataset,table_name,land_dataset,land_table in zip(self.dataset_name,self.table,self.landing_dataset_name,self.landing_table):
    # print(dataset)
      target_df = read_table(self.storage, self.container, dataset,table_name,self.file_type)
      landing_df = read_table(self.landing_storage, self.landing_container, land_dataset, land_table,self.landing_file_type)
    
    # target_df = read_table(self.storage, self.container, self.dataset_name, self.table,self.file_type)
      target_df = target_df.drop('__hash_key', '__record_created_datetime_utc', '__record_created_datetime_cst','__source_server','__source_database','__source_table')
   
    # landing_df = read_table(self.landing_storage, self.landing_container, self.landing_dataset_name, self.landing_table,self.landing_file_type)
      landing_schema_temp = landing_df.schema
      target_schema_temp = target_df.schema      

      landing_schema.append(landing_schema_temp)      
      target_schema.append(target_schema_temp) 
      # print(target_schema)
    # print(target_schema)
    for landing,target in zip(landing_schema,target_schema):
      # print(landing,target)
      self.assertEqual(landing, target, f'Expected Schema {landing}, but got {target}')
      

  def test_check_null_pk(self):

    for dataset,table_name,land_dataset,land_table,key in zip(self.dataset_name,self.table,self.landing_dataset_name,self.landing_table,self.primary_keys):
      # print(dataset)
      target_df = read_table(self.storage, self.container, dataset,table_name,self.file_type)
      landing_df = read_table(self.landing_storage,self.landing_container, land_dataset, land_table,self.landing_file_type) 
      if landing_df.filter(landing_df[key].isNull()).count() > 0 or target_df.filter(target_df[key].isNull()).count() > 0:
          pk_null_check = True
      else:
          pk_null_check = False
          
   
      self.assertFalse(pk_null_check)

  def test_check_null_count(self):    

    for dataset,table_name,land_dataset,land_table in zip(self.dataset_name,self.table,self.landing_dataset_name,self.landing_table):
      target_df = read_table(self.storage, self.container, dataset,table_name,self.file_type)
      landing_df = read_table(self.landing_storage, self.landing_container, land_dataset, land_table,self.landing_file_type) 
      target_df = target_df.drop('__hash_key', '__record_created_datetime_utc', '__record_created_datetime_cst','__source_server','__source_database','__source_table')
  
      # Calculate null counts for each column in Landing and Bronze 

      target_null_counts = {col: target_df.filter(target_df[col].isNull()).count() for col in target_df.columns}
      landing_null_counts = {col: landing_df.filter(landing_df[col].isNull()).count() for col in landing_df.columns}

# Calculate the total number of nulls across all columns
      target_total_nulls = sum(target_null_counts.values())
      landing_total_nulls = sum(landing_null_counts.values())
      
      if target_total_nulls == landing_total_nulls:
          null_check = True
      else:
          null_check = False
      null_check
      # print(null_check)

      self.assertTrue(null_check)


run_test = unittest.main(argv=[' '], verbosity=10, exit=False)
 
# Check if the tests were successful
assert run_test.result.wasSuccessful(), 'Tests Failed, see the logs below for further details'
