# Databricks notebook source
storage_account_key = dbutils.secrets.get("CarenetADLSScope","xxxx")

# Configure Azure Storage account key
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format('carenetdemodatalake'),
    storage_account_key)

# COMMAND ----------

datedetails_result_df = spark.sql("""
        --Create or replace temporary view datedetails as
        SELECT
            replace(calendar_date,'-','') as DateKey,
            CAST(calendar_date AS DATE)  as DateFull,
            year(calendar_date) as Year,
            month(calendar_date) as Month,
            day(calendar_date) as Day,
            dayofweek(calendar_date) as DayOfWeek,
            date_format(calendar_date, 'EEEE') as DayName,
            date_format(calendar_date,'MMMM') as MonthName,
            weekofyear(calendar_date) as Week,
            to_date(date_trunc('week', calendar_date), 'yyyy-MM-dd') as StartOfWeek,
            to_date(date_add(day, -1, (date_add(week, 1, cast(date_trunc('week', calendar_date) as TIMESTAMP)))), 'yyyy-MM-dd') as EndOfWeek,
            to_date(date_trunc('year', calendar_date), 'yyyy-MM-dd') as StartOfYear,
            to_date(date_add(day, -1, (date_add(year, 1, cast(date_trunc('year', calendar_date) as TIMESTAMP)))), 'yyyy-MM-dd') as EndOfYear,
            dayofyear(calendar_date) AS DayOfYear,
            quarter(calendar_date) as Quarter,
            to_date(date_trunc('quarter', calendar_date), 'yyyy-MM-dd') as StartOfQuarter,
            to_date(date_add(day, -1, (date_add(quarter, 1, cast(date_trunc('quarter', calendar_date) as TIMESTAMP)))), 'yyyy-MM-dd') as EndOfQuarter,
            to_date(date_trunc('month', calendar_date), 'yyyy-MM-dd') as StartOfMonth,
            to_date(date_add(day, -1, (date_add(month, 1, cast(date_trunc('month', calendar_date) as TIMESTAMP)))), 'yyyy-MM-dd') as EndOfMonth,
            date_format(calendar_date, 'MMM yyyy') AS YearMonth,
            date_format(calendar_date, 'yyyyMM') AS YearMonthCode,
            CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN 0 ELSE 1 END as IsWeekday,
            CASE WHEN year(calendar_date) % 4 = 0 THEN 1 ELSE 0 END as IsLeapyear,
            CASE WHEN  month(calendar_date) = 3 and date_format(calendar_date, 'EEEE') = 'Sunday' and (day(calendar_date) between 8 and 14) THEN 1 ELSE 0 END as IsDSTStartDate,
            CASE WHEN  month(calendar_date) = 11 and date_format(calendar_date, 'EEEE') = 'Sunday' and (day(calendar_date) between 1 and 7) THEN 1 ELSE 0 END as IsDSTEndDate,
            CASE WHEN  month(calendar_date) = 12 and day(calendar_date) = 31 and date_format(calendar_date, 'EEEE') = 'Friday' THEN 'New Year Day'
                WHEN  month(calendar_date) = 1 and day(calendar_date) = 1 and dayofweek(calendar_date) IN (2,3,4,5,6) THEN 'New Year Day'
                WHEN  month(calendar_date) = 1 and day(calendar_date) = 2 and date_format(calendar_date, 'EEEE') = 'Monday' THEN 'New Year Day'
                WHEN  month(calendar_date) = 1 and date_format(calendar_date, 'EEEE') = 'Monday' and (day(calendar_date) between 15 and 21) THEN 'MLK Day'
                WHEN  month(calendar_date) = 2 and date_format(calendar_date, 'EEEE') = 'Monday' and (day(calendar_date) between 15 and 21) THEN 'President Day'
                WHEN  month(calendar_date) = 5 and date_format(calendar_date, 'EEEE') = 'Monday' and month(date_add(calendar_date, 7)) = 6 THEN 'Memorial Day'
                WHEN  month(calendar_date) = 6 and day(calendar_date) = 18 and date_format(calendar_date, 'EEEE') = 'Friday' THEN 'Juneteenth National Independence Day'
                WHEN  month(calendar_date) = 6 and day(calendar_date) = 19 and dayofweek(calendar_date) IN (2,3,4,5,6) THEN 'Juneteenth National Independence Day'
                WHEN  month(calendar_date) = 6 and day(calendar_date) = 20 and date_format(calendar_date, 'EEEE') = 'Monday' THEN 'Juneteenth National Independence Day'
                WHEN  month(calendar_date) = 7 and day(calendar_date) = 3 and date_format(calendar_date, 'EEEE') = 'Friday' THEN 'Independence Day'
                WHEN  month(calendar_date) = 7 and day(calendar_date) = 4 and dayofweek(calendar_date) IN (2,3,4,5,6) THEN 'Independence Day'
                WHEN  month(calendar_date) = 7 and day(calendar_date) = 5 and date_format(calendar_date, 'EEEE') = 'Monday' THEN 'Independence Day'
                WHEN  month(calendar_date) = 9 and date_format(calendar_date, 'EEEE') = 'Monday' and (day(calendar_date) between 1 and 7) THEN 'labor Day'
                WHEN  month(calendar_date) = 10 and date_format(calendar_date, 'EEEE') = 'Monday' and (day(calendar_date) between 8 and 14) THEN 'Columbus Day'
                WHEN  month(calendar_date) = 11 and day(calendar_date) = 10 and date_format(calendar_date, 'EEEE') = 'Friday' THEN 'Veterans Day'
                WHEN  month(calendar_date) = 11 and day(calendar_date) = 11 and dayofweek(calendar_date) IN (2,3,4,5,6) THEN 'Veterans Day'
                WHEN  month(calendar_date) = 11 and day(calendar_date) = 12 and date_format(calendar_date, 'EEEE') = 'Monday' THEN 'Veterans Day'
                WHEN  month(calendar_date) = 11 and date_format(calendar_date, 'EEEE') = 'Thursday' and (day(calendar_date) between 22 and 28) THEN 'Thanksgiving Day'
                WHEN  month(calendar_date) = 11 and date_format(calendar_date, 'EEEE') = 'Friday' and (day(calendar_date) between 22 and 28) THEN 'Day after Thanksgiving Day'
                WHEN  month(calendar_date) = 12 and day(calendar_date) = 24 and date_format(calendar_date, 'EEEE') = 'Friday' THEN 'Christmas Day'
                WHEN  month(calendar_date) = 12 and day(calendar_date) = 25 and dayofweek(calendar_date) IN (2,3,4,5,6) THEN 'Christmas Day'
                WHEN  month(calendar_date) = 12 and day(calendar_date) = 26 and date_format(calendar_date, 'EEEE') = 'Monday' THEN 'Christmas Day'

            ELSE '' END as USFederalHoliday

        FROM (
                SELECT explode(sequence(DATE'2020-01-01', DATE'2050-12-31', INTERVAL 1 DAY)) as  calendar_date
            ) as dates

        UNION ALL

        SELECT
            '19000101' as DateKey,
            '1900-01-01' as DateFull,
            1900 as Year,
            1 as Month,
            1 as Day,
            2 as DayOfWeek,
            'Monday' as DayName,
            'January' as MonthName,
            1 as Week,
            '1899-12-31' as StartOfWeek,
            '1900-01-06' as EndOfWeek,
            '1900-01-01' as StartOfYear,
            '1900-12-31' as EndOfYear,
            1 AS DayOfYear,
            1 as Quarter,
            '1900-01-01' as StartOfQuarter,
            '1900-03-31' as EndOfQuarter,
            '1900-01-01' as StartOfMonth,
            '1900-01-31' as EndOfMonth,
            'Jan 1900' AS YearMonth,
            '190001' AS YearMonthCode,
            1 IsWeekday,
            1 IsLeapyear,
            0 IsDSTStartDate,
            0 IsDSTEndDate,
            '' USFederalHoliday;""")
datedetails_result_df.display()    
datedetails_result_df.createOrReplaceGlobalTempView("datedetails")   

      

        

# COMMAND ----------

result_df = spark.sql("""
        --Create or Replace table silver.dim_date
        --as
        select a.*
        , case when a.IsWeekday = 1 and a.USFederalHoliday not in ('New Year Day', 'Memorial Day', 'Independence Day', 'labor Day', 'Thanksgiving Day', 'Day after Thanksgiving Day', 'Christmas Day') then 1 else 0 end as IsCarenetWorkday
        , case when a.DateFull between b.DateFull and c.DateFull then -4 else -5 end as EasternOffSet
        , case when a.DateFull between b.DateFull and c.DateFull then -5 else -6 end as CentralOffSet
        , case when a.DateFull between b.DateFull and c.DateFull then -6 else -7 end as MountainOffSet
        , -7 as ArizonaOffSet
        , case when a.DateFull between b.DateFull and c.DateFull then -7 else -8 end as PacificOffSet
        , case when a.DateFull between b.DateFull and c.DateFull then -8 else -9 end as AlaskaOffSet
        , -10 as HawaiiAleutianOffSet

        from datedetails a
        join
            (select distinct year, DateFull
            from datedetails where IsDSTStartDate = 1 ) b 
        on a.year = b.year
        join
            (select distinct year, DateFull
            from datedetails where IsDSTEndDate = 1 ) c
        on a.year = c.year""")

# COMMAND ----------

result_df.display()

# COMMAND ----------


#result_df.display()

result_df.write.format("delta").mode("overwrite").save("abfss://silver@carenetdemodatalake.dfs.core.windows.net/date_table")
result_df.write.format("delta").mode("overwrite").save("abfss://pregold@carenetdemodatalake.dfs.core.windows.net/Dim_date")


# COMMAND ----------

spark.catalog.clearCache()