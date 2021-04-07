-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.notebook.run("Extract", 0)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS COVID_INGESTION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **country_vaccinations**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_country_vaccinations (
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
LOCATION "/mnt/kaggle/Covid/Ingestion/covid_19_world_vaccination_progress/country_vaccinations.csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **covid_DE**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_COVID_DE (
state STRING,
country STRING,
age_group STRING,
gender STRING,
date STRING,
cases STRING,
death STRING,
recovered STRING) 
USING CSV 
OPTIONS ("header" True)
LOCATION "/mnt/kaggle/Covid/Ingestion/covid19-tracking-germany/covid_de.csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Demographics_DE**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_DEMOGRAPHICS_DE (
STATE STRING,
GENDER STRING,
AGE_GROUP STRING,
POPULATION STRING)
USING CSV
OPTIONS ("header" True)
LOCATION '/mnt/kaggle/Covid/Ingestion/covid19-tracking-germany/demographics_de.csv'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Worldometer coronavirus daily data**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_Worldometer_coronavirus_daily_data (
DATE STRING,
COUNTRY STRING,
CUMULATIVE_TOTAL_CASES STRING,
DAILY_NEW_CASES STRING,
ACTIVE_CASES STRING,
CUMULATIVE_TOTAL_DEATHS STRING,
DAILY_NEW_DEATHS STRING)
USING CSV
OPTIONS ("header" True)
LOCATION '/mnt/kaggle/Covid/Ingestion/covid19-global-dataset/worldometer_coronavirus_daily_data.csv'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **worldometer_coronavirus_summary_data**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_worldometer_coronavirus_summary_data (
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
LOCATION "/mnt/kaggle/Covid/Ingestion/covid19-global-dataset/worldometer_coronavirus_summary_data.csv"
