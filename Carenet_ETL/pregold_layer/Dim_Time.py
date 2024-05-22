# Databricks notebook source
storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxx")

# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format('carenetdemodatalake'),
    storage_account_key)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_time table
# MAGIC CREATE OR REPLACE TABLE silver.dim_time AS
# MAGIC SELECT 
# MAGIC     (hours * 10000) + (minutes * 100) + seconds AS TimeKey,
# MAGIC     hours AS Hour24,
# MAGIC     LPAD(CAST(hours AS STRING), 2, '0') AS Hour24ShortString,
# MAGIC     CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':00') AS Hour24MinString,
# MAGIC     CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':00:00') AS Hour24FullString,
# MAGIC     hours % 12 AS Hour12,
# MAGIC     LPAD(CAST(hours % 12 AS STRING), 2, '0') AS Hour12ShortString,
# MAGIC     CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':00') AS Hour12MinString,
# MAGIC     CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':00:00') AS Hour12FullString,
# MAGIC     CASE WHEN hours < 12 THEN 0 ELSE 1 END  AS AmPmCode,
# MAGIC     CASE WHEN hours < 12 THEN 'AM' ELSE 'PM' END AS AmPmString,
# MAGIC     minutes AS Minute,
# MAGIC     (hours * 100) + minutes AS MinuteCode,
# MAGIC     LPAD(CAST(minutes AS STRING), 2, '0') AS MinuteShortString,
# MAGIC     CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':00') AS MinuteFullString24,
# MAGIC     CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':00') AS MinuteFullString12,
# MAGIC     CASE WHEN minutes < 30 THEN 0 ELSE 1 END AS HalfHour,
# MAGIC     (hours * 100) + (CASE WHEN minutes <30 THEN 0 ELSE 30 END)  AS HalfHourCode,
# MAGIC     LPAD(CAST((hours * 100) + (CASE WHEN minutes <30 THEN 0 ELSE 30 END) AS STRING), 4, '0') AS HalfHourShortString,
# MAGIC     CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes <30 THEN 0 ELSE 30 END AS STRING), 2, '0'), ':00') AS HalfHourFullString24,
# MAGIC     CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes <30 THEN 0 ELSE 30 END AS STRING), 2, '0'), ':00') AS HalfHourFullString12,
# MAGIC      CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 1 WHEN minutes < 45 THEN 2 ELSE 3 END AS QuarterHour,
# MAGIC       (hours * 100) + (CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END) AS QuarterHourCode,
# MAGIC     LPAD(CAST((hours * 100) + (CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END) AS STRING), 4, '0')  AS QuarterHourShortString,
# MAGIC     CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END AS STRING), 2, '0'), ':00') AS QuarterHourFullString24,
# MAGIC     CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END AS STRING), 2, '0'), ':00') AS QuarterHourFullString12,
# MAGIC     seconds AS Second,
# MAGIC     LPAD(CAST(seconds AS STRING), 2, '0') AS SecondShortString,
# MAGIC     CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':', LPAD(CAST(seconds AS STRING), 2, '0')) AS FullTimeString24,
# MAGIC     CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':', LPAD(CAST(seconds AS STRING), 2, '0')) AS FullTimeString12
# MAGIC
# MAGIC FROM (
# MAGIC     SELECT EXPLODE(sequence(0, 23)) AS hours
# MAGIC ) AS h
# MAGIC CROSS JOIN (
# MAGIC     SELECT EXPLODE(sequence(0, 59)) AS minutes
# MAGIC ) AS m
# MAGIC CROSS JOIN (
# MAGIC     SELECT EXPLODE(sequence(0, 59)) AS seconds
# MAGIC ) AS s
# MAGIC
# MAGIC /*
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     999999 AS TimeKey,
# MAGIC     99 AS Hour24,
# MAGIC     '99' AS Hour24ShortString,
# MAGIC     '99:00' AS Hour24MinString,
# MAGIC     '99:00:00' AS Hour24FullString,
# MAGIC     99 AS Hour12,
# MAGIC     '99' AS Hour12ShortString,
# MAGIC     '99:00' AS Hour12MinString,
# MAGIC     '99:00:00' AS Hour12FullString,
# MAGIC     0 AS AmPmCode,
# MAGIC     'AM' AS AmPmString,
# MAGIC     99 AS Minute,
# MAGIC     99 AS MinuteCode,
# MAGIC     '99' AS MinuteShortString,
# MAGIC     '99:99:00' AS MinuteFullString24,
# MAGIC     '99:99:00' AS MinuteFullString12,
# MAGIC     0 AS HalfHour,
# MAGIC     9900  AS HalfHourCode,
# MAGIC     '9900' AS HalfHourShortString,
# MAGIC     '99:00:00' AS HalfHourFullString24,
# MAGIC     '99:00:00' HalfHourFullString12,
# MAGIC     0 AS QuarterHour,
# MAGIC     9900 AS QuarterHourCode,
# MAGIC     '9900' AS QuarterHourShortString,
# MAGIC     '99:00:00' AS QuarterHourFullString24,
# MAGIC     '99:00:00' AS QuarterHourFullString12,
# MAGIC     99 AS Second,
# MAGIC     '99' AS SecondShortString,
# MAGIC     '99:99:99' FullTimeString24,
# MAGIC     '99:99:99' AS FullTimeString12
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.dim_time
# MAGIC ORDER BY TimeKey desc 
# MAGIC

# COMMAND ----------

time_df = spark.sql("""        
        /*CREATE OR REPLACE TABLE silver.dim_time AS*/
            SELECT 
                (hours * 10000) + (minutes * 100) + seconds AS TimeKey,
                hours AS Hour24,
                LPAD(CAST(hours AS STRING), 2, '0') AS Hour24ShortString,
                CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':00') AS Hour24MinString,
                CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':00:00') AS Hour24FullString,
                hours % 12 AS Hour12,
                LPAD(CAST(hours % 12 AS STRING), 2, '0') AS Hour12ShortString,
                CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':00') AS Hour12MinString,
                CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':00:00') AS Hour12FullString,
                CASE WHEN hours < 12 THEN 0 ELSE 1 END  AS AmPmCode,
                CASE WHEN hours < 12 THEN 'AM' ELSE 'PM' END AS AmPmString,
                minutes AS Minute,
                (hours * 100) + minutes AS MinuteCode,
                LPAD(CAST(minutes AS STRING), 2, '0') AS MinuteShortString,
                CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':00') AS MinuteFullString24,
                CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':00') AS MinuteFullString12,
                CASE WHEN minutes < 30 THEN 0 ELSE 1 END AS HalfHour,
                (hours * 100) + (CASE WHEN minutes <30 THEN 0 ELSE 30 END)  AS HalfHourCode,
                LPAD(CAST((hours * 100) + (CASE WHEN minutes <30 THEN 0 ELSE 30 END) AS STRING), 4, '0') AS HalfHourShortString,
                CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes <30 THEN 0 ELSE 30 END AS STRING), 2, '0'), ':00') AS HalfHourFullString24,
                CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes <30 THEN 0 ELSE 30 END AS STRING), 2, '0'), ':00') AS HalfHourFullString12,
                CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 1 WHEN minutes < 45 THEN 2 ELSE 3 END AS QuarterHour,
                (hours * 100) + (CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END) AS QuarterHourCode,
                LPAD(CAST((hours * 100) + (CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END) AS STRING), 4, '0')  AS QuarterHourShortString,
                CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END AS STRING), 2, '0'), ':00') AS QuarterHourFullString24,
                CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(CASE WHEN minutes < 15 THEN 0 WHEN minutes < 30 THEN 15 WHEN minutes < 45 THEN 30 ELSE 45 END AS STRING), 2, '0'), ':00') AS QuarterHourFullString12,
                seconds AS Second,
                LPAD(CAST(seconds AS STRING), 2, '0') AS SecondShortString,
                CONCAT(LPAD(CAST(hours AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':', LPAD(CAST(seconds AS STRING), 2, '0')) AS FullTimeString24,
                CONCAT(LPAD(CAST(hours % 12 AS STRING), 2, '0'), ':', LPAD(CAST(minutes AS STRING), 2, '0'), ':', LPAD(CAST(seconds AS STRING), 2, '0')) AS FullTimeString12

            FROM (
                SELECT EXPLODE(sequence(0, 23)) AS hours
            ) AS h
            CROSS JOIN (
                SELECT EXPLODE(sequence(0, 59)) AS minutes
            ) AS m
            CROSS JOIN (
                SELECT EXPLODE(sequence(0, 59)) AS seconds
            ) AS s
            
                           """)

# COMMAND ----------

time_df.display()

# COMMAND ----------

time_df.write.format("delta").mode("overwrite").save("abfss://silver@carenetdemodatalake.dfs.core.windows.net/Time")
time_df.write.format("delta").mode("overwrite").save("abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_time")


# COMMAND ----------

