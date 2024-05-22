# Databricks notebook source
import json

def get_incremental_tables():
    try:
        incremental_table_list = []
        # Get tables info with last inserted datetime value from current load process flow
        with open(f'/Workspace/Shared/Configuration/last_inserted_date_time.json', 'r') as json_file:
            input_parameter_sets = json.load(json_file)
            for table in input_parameter_sets:
                incremental_table_list.append(table)
        return incremental_table_list
    except Exception as e:
        print(f"An error occurred while getting incremental tables: {str(e)}")
        return None

incremental_table_list = get_incremental_tables()
print(incremental_table_list)



# COMMAND ----------



def update_config_json(incremental_table_list):
    try:
        for table in incremental_table_list:
            with open(f'/Workspace/Shared/Configuration/{table}.json', 'r') as config_json_file:
                config_parameter_sets = json.load(config_json_file)
            with open(f'/Workspace/Shared/Configuration/last_inserted_date_time.json', 'r') as input_json_file:
                input_parameter_sets = json.load(input_json_file)
            for config_table_info in config_parameter_sets[table]:
                for table_info in input_parameter_sets[table]:
                    if config_table_info["host_name"] == table_info[0]:
                        config_table_info["last_inserted_datetime"] = table_info[2]
            updated_config_json = json.dumps(config_parameter_sets, indent=2)
            with open(f'/Workspace/Shared/Configuration/{table}.json', 'w') as json_file:
                json_file.write(updated_config_json)
                print("Config files updated successfully")
    except Exception as e:
        print(f"An error occurred while updating config JSON for table {table_name}: {str(e)}")

update_config_json(incremental_table_list)

