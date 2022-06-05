# Databricks notebook source
# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS COVID_INGESTION

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history (
# MAGIC file_name STRING,
# MAGIC last_commit_message STRING,
# MAGIC last_load_ts TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Ingestion/TBL_csse_covid_19_daily_reports_load_history/'
