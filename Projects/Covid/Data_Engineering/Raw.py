# Databricks notebook source
# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS COVID_RAW

# COMMAND ----------

# MAGIC %md
# MAGIC **csse_covid_19_daily_reports**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_csse_covid_19_daily_reports
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_csse_covid_19_daily_reports/'

# COMMAND ----------

# MAGIC %md
# MAGIC **germany_vaccinations_timeseries_v2**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_germany_vaccinations_timeseries_v2
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_germany_vaccinations_timeseries_v2/'

# COMMAND ----------

# MAGIC %md
# MAGIC **germany_deliveries_timeseries_v2**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_germany_deliveries_timeseries_v2
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_germany_deliveries_timeseries_v2/'

# COMMAND ----------

# MAGIC %md
# MAGIC **germany_vaccinations_by_state_v1**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_germany_vaccinations_by_state_v1
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_germany_vaccinations_by_state_v1/'

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Altersgruppen**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Altersgruppen
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_RKI_Altersgruppen/'

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_COVID19**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_COVID19
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_RKI_COVID19/'

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Corona_Landkreise**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Corona_Landkreise
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_RKI_Corona_Landkreise/'

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Corona_Bundeslaender**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Corona_Bundeslaender
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_RKI_Corona_Bundeslaender/'

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Data_Status**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Data_Status
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_RKI_Data_Status/'

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_key_data**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_key_data
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_RKI_key_data/'

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_history**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_history
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Raw/TBL_RKI_history/'
