-- Databricks notebook source
-- MAGIC %python
-- MAGIC %pip install kaggle

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.system(f"""export KAGGLE_USERNAME={dbutils.secrets.get('KAGGLE', 'KAGGLE_USERNAME')}""")
-- MAGIC os.system(f"""export KAGGLE_KEY={dbutils.secrets.get('KAGGLE', 'KAGGLE_KEY')}""")
-- MAGIC os.system('kaggle datasets download gpreda/covid-world-vaccination-progress -p /dbfs/tmp/ --force')
-- MAGIC os.system('kaggle datasets download headsortails/covid19-tracking-germany -p /dbfs/tmp/ --force')
-- MAGIC os.system('kaggle datasets download josephassaker/covid19-global-dataset -p /dbfs/tmp/ --force')
-- MAGIC os.system('unzip -o /tmp/covid-world-vaccination-progress.zip -d /dbfs/mnt/kaggle/Covid/Bronze/covid_19_world_vaccination_progress/')
-- MAGIC os.system('unzip -o /tmp/covid19-tracking-germany.zip -d /dbfs/mnt/kaggle/Covid/Bronze/covid19-tracking-germany/')
-- MAGIC os.system('unzip -o /tmp/covid19-global-dataset.zip -d /dbfs/mnt/kaggle/Covid/Bronze/covid19-global-dataset/')

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS COVID

-- COMMAND ----------

USE COVID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##BRONZE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **country_vaccinations**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_country_vaccinations_bronze (
    country STRING,
    iso_code STRING,
    date STRING,
    total_vaccinations STRING,
    people_vaccinated STRING,
    people_fully_vaccinated STRING,
    daily_vaccinations_raw STRING,
    daily_vaccinations STRING,
    total_vaccinations_per_hundred STRING,
    people_vaccinated_per_hundred STRING,
    people_fully_vaccinated_per_hundred STRING,
    daily_vaccinations_per_million STRING,
    vaccines STRING,
    source_name STRING,
    source_website STRING
) USING CSV 
OPTIONS ("header" True)
LOCATION "/mnt/kaggle/Covid/Bronze/covid_19_world_vaccination_progress/country_vaccinations.csv"

-- COMMAND ----------

REFRESH TABLE TBL_country_vaccinations_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **covid_DE**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_COVID_DE_BRONZE (
state STRING,
country STRING,
age_group STRING,
gender STRING,
date STRING,
cases STRING,
death STRING,
recoverd STRING) 
USING CSV 
OPTIONS ("header" True)
LOCATION "/mnt/kaggle/Covid/Bronze/covid19-tracking-germany/covid_de.csv"

-- COMMAND ----------

REFRESH TABLE TBL_country_vaccinations_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Demographics_DE**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_DEMOGRAPHICS_DE_BRONZE (
STATE STRING,
GENDER STRING,
AGE_GROUP STRING,
POPULATION STRING)
USING CSV
OPTIONS ("header" True)
LOCATION '/mnt/kaggle/Covid/Bronze/covid19-tracking-germany/demographics_de.csv'

-- COMMAND ----------

REFRESH TABLE TBL_DEMOGRAPHICS_DE_BRONZE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Worldometer coronavirus daily data**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_Worldometer_coronavirus_daily_data_bronze (
DATE STRING,
COUNTRY STRING,
CUMULATIVE_TOTAL_CASES STRING,
DAILY_NEW_CASES STRING,
ACTIVE_CASES STRING,
CUMULATIVE_TOTAL_DEATHS STRING,
DAILY_NEW_DEATHS STRING)
USING CSV
OPTIONS ("header" True)
LOCATION '/mnt/kaggle/Covid/Bronze/covid19-global-dataset/worldometer_coronavirus_daily_data.csv'

-- COMMAND ----------

REFRESH TABLE TBL_Worldometer_coronavirus_daily_data_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **worldometer_coronavirus_summary_data**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_worldometer_coronavirus_summary_data_BRONZE (
country STRING,
CONTINENT STRING,
TOTAL_CONFIRMED STRING,
TOTAL_DEATHS STRING,
TOTAL_RECOVERED STRING,
ACTIVE_CASES STRING,
SERIOUS_OR_CRITICAL STRING,
TOTAL_CASES_PER_LM_POPULATION STRING,
TOTAL_TESTS STRING,
TOTAL_TESTS_PER_LM_POPULATION STRING,
POPULATION STRING)
USING CSV
OPTIONS ("header" True)
LOCATION "/mnt/kaggle/Covid/Bronze/covid19-global-dataset/worldometer_coronavirus_summary_data.csv"

-- COMMAND ----------

REFRESH TABLE TBL_worldometer_coronavirus_summary_data_BRONZE

-- COMMAND ----------


