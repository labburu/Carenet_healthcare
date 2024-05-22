# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Get Last inserted date time for each of the full load tables from the current execution

# COMMAND ----------

import json

def get_full_table_list_with_date():
    try:
        full_table_list_with_date = []
        # Get tables info with last inserted datetime value from current load process flow
        with open(f'/Workspace/Shared/Configuration/last_inserted_date_time_full.json', 'r') as json_file:
            input_parameter_sets = json.load(json_file)
            for table in input_parameter_sets:
                full_table_list_with_date.append(table)
        return full_table_list_with_date
    except Exception as e:
        raise Exception(f"An error occurred while reading JSON file: {str(e)}")


full_table_list = get_full_table_list_with_date()
print(full_table_list)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Update table Config files with last inserted date time

# COMMAND ----------


def update_json_files(table_list):
    try:
        for table in table_list:
            with open(f'/Workspace/Shared/Configuration/{table}.json', 'r') as config_json_file:
                config_parameter_sets = json.load(config_json_file)
            with open(f'/Workspace/Shared/Configuration/last_inserted_date_time_full.json', 'r') as input_json_file:
                input_parameter_sets = json.load(input_json_file)
            for config_table_info in config_parameter_sets[table]:
                for table_info in input_parameter_sets[table]:
                    if config_table_info["host_name"] == table_info[0]:
                        config_table_info["last_inserted_datetime"] = table_info[2]
            updated_config_json = json.dumps(config_parameter_sets, indent=2)
            with open(f'/Workspace/Shared/Configuration/{table}.json', 'w') as json_file:
                json_file.write(updated_config_json)
            print("JSON files updated successfully.")
    except Exception as e:
        raise Exception(f"An error occurred while updating JSON files: {str(e)}")

# Call the function
update_json_files(full_table_list)

# COMMAND ----------


