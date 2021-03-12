-- Databricks notebook source
-- MAGIC %python
-- MAGIC import os
-- MAGIC 
-- MAGIC class kaggle_interface:
-- MAGIC   
-- MAGIC   def __init__(self, user_name, key):
-- MAGIC     self.user_name = user_name
-- MAGIC     self.key = key
-- MAGIC     os.environ['KAGGLE_USERNAME'] = self.user_name
-- MAGIC     os.environ['KAGGLE_KEY'] = self.key
-- MAGIC     from kaggle.api.kaggle_api_extended import KaggleApi
-- MAGIC     self.api = KaggleApi()
-- MAGIC     self.api.authenticate()
-- MAGIC   
-- MAGIC   def download_dataset(self, dataset_name, destination, backup = True):
-- MAGIC     self.api.dataset_download_files(dataset_name, path = '/dbfs/tmp/', force = True)
-- MAGIC     self.__create_target_folder(destination)
-- MAGIC     self.__unzip_into_target(dataset_name, destination)
-- MAGIC     if backup:
-- MAGIC       os.system(f"""unzip -o /dbfs/tmp/{dataset_name.split('/')[1]}.zip -d /dbfs/{destination}{self.__get_dataset_metadata(dataset_name).strftime("%Y-%m-%d")}/""")
-- MAGIC     
-- MAGIC   def __get_dataset_metadata(self, dataset_name):
-- MAGIC     datasets=self.api.dataset_list(search=dataset_name)
-- MAGIC     dataset_info = {str(dataset):dataset.lastUpdated for dataset in datasets}
-- MAGIC     return dataset_info[dataset_name]
-- MAGIC   
-- MAGIC   def __create_target_folder(self, destination):
-- MAGIC     dbutils.fs.mkdirs(destination)
-- MAGIC     
-- MAGIC     
-- MAGIC   def __unzip_into_target(self, dataset_name, destination):
-- MAGIC     os.system(f"""unzip -o /dbfs/tmp/{dataset_name.split('/')[1]}.zip -d /dbfs/{destination}""")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC class data_lake:
-- MAGIC   
-- MAGIC   def __init__(self, archive_path, ingestion_path, raw_path, qualified_path, curated_path):
-- MAGIC     self.archive_path = archive_path
-- MAGIC     self.ingestion_path = ingestion_path
-- MAGIC     self.raw_path = raw_path
-- MAGIC     self.qualified_path = qualified_path
-- MAGIC     self.curated_path = curated_path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC from datetime import datetime
-- MAGIC 
-- MAGIC statinfo = os.stat('/dbfs/mnt/kaggle/Covid/Bronze/covid_19_world_vaccination_progress/country_vaccinations.csv')
-- MAGIC modified_date = datetime.fromtimestamp(statinfo.st_mtime)
-- MAGIC print(modified_date)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC interface = kaggle_interface(dbutils.secrets.get('KAGGLE', 'KAGGLE_USERNAME'), dbutils.secrets.get('KAGGLE', 'KAGGLE_KEY'))
-- MAGIC interface.download_dataset('gpreda/covid-world-vaccination-progress', 'mnt/kaggle/Covid/Ingestion/covid_19_world_vaccination_progress/', False)
-- MAGIC interface.download_dataset('headsortails/covid19-tracking-germany', 'mnt/kaggle/Covid/Ingestion/covid19-tracking-germany/', False)
-- MAGIC interface.download_dataset('josephassaker/covid19-global-dataset', 'mnt/kaggle/Covid/Ingestion/covid19-global-dataset/', False)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS COVID

-- COMMAND ----------

USE COVID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **country_vaccinations**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_country_vaccinations_INGESTION (
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

CREATE TABLE IF NOT EXISTS TBL_COVID_DE_INGESTION (
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
LOCATION "/mnt/kaggle/Covid/Ingestion/covid19-tracking-germany/covid_de.csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Demographics_DE**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_DEMOGRAPHICS_DE_INGESTION (
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

CREATE TABLE IF NOT EXISTS TBL_Worldometer_coronavirus_daily_data_INGESTION (
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

CREATE TABLE IF NOT EXISTS TBL_worldometer_coronavirus_summary_data_INGESTION (
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Raw

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_country_vaccinations_RAW (
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
    source_website STRING,
    INSERT_TS STRING,
    UPDATE_TS STRING
) USING DELTA
LOCATION "/mnt/kaggle/Covid/Raw/covid_19_world_vaccination_progress/"

-- COMMAND ----------

MERGE INTO TBL_country_vaccinations_RAW AS T
USING 
TBL_country_vaccinations_INGESTION S
on 
T.country = S.country AND
T.iso_code = S.iso_code AND
T.date = S.date AND
T.total_vaccinations = S.total_vaccinations AND
T.people_vaccinated = S.people_vaccinated AND
T.people_fully_vaccinated = S.people_fully_vaccinated AND
T.daily_vaccinations_raw = S.daily_vaccinations_raw AND
T.daily_vaccinations = S.daily_vaccinations AND
T.total_vaccinations_per_hundred = S.total_vaccinations_per_hundred AND
T.people_vaccinated_per_hundred = S.people_vaccinated_per_hundred AND
T.people_fully_vaccinated_per_hundred = S.people_fully_vaccinated_per_hundred AND
T.daily_vaccinations_per_million = S.daily_vaccinations_per_million AND
T.vaccines = S.vaccines AND
T.source_name = S.source_name AND
T.source_website = S.source_website
WHEN NOT MATCHED
THEN INSERT ( country,
iso_code,
date,
total_vaccinations,
people_vaccinated,
people_fully_vaccinated,
daily_vaccinations_raw,
daily_vaccinations,
total_vaccinations_per_hundred,
people_vaccinated_per_hundred,
people_fully_vaccinated_per_hundred,
daily_vaccinations_per_million,
vaccines,
source_name,
source_website, insert_ts, update_ts ) 
VALUES (
S.country,
S.iso_code,
S.date,
S.total_vaccinations,
S.people_vaccinated,
S.people_fully_vaccinated,
S.daily_vaccinations_raw,
S.daily_vaccinations,
S.total_vaccinations_per_hundred,
S.people_vaccinated_per_hundred,
S.people_fully_vaccinated_per_hundred,
S.daily_vaccinations_per_million,
S.vaccines,
S.source_name,
S.source_website, 
current_timestamp, current_Timestamp)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **country_mapping_table**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TBL_COUNTRY_MAPPING_RAW USING DELTA LOCATION '/mnt/kaggle/Covid/Raw/mapping/'
AS 
SELECT trim(country) as target_country_name, trim(country) as source_country_name, current_timestamp as INSERT_TS, current_timestamp as UPDATE_TS FROM (
SELECT distinct upper(trim(country)) as country FROM tbl_country_vaccinations_Ingestion
UNION
SELECT distinct upper(trim(country)) as country FROM tbl_worldometer_coronavirus_summary_data_Ingestion)

-- COMMAND ----------

MERGE INTO TBL_COUNTRY_MAPPING_Ingestion AS T
USING 
(SELECT trim(country) as target_country_name, trim(country) as source_country_name, current_timestamp as INSERT_TS, current_timestamp as UPDATE_TS FROM (
SELECT distinct upper(trim(country)) as country FROM tbl_country_vaccinations_Ingestion
UNION
SELECT distinct upper(trim(country)) as country FROM tbl_worldometer_coronavirus_summary_data_Ingestion)) S
on T.source_country_name = S.source_country_name
WHEN NOT MATCHED
THEN INSERT ( target_Country_name, source_Country_name, insert_ts, update_ts ) VALUES (S.target_country_name, S.source_country_name, current_timestamp, current_Timestamp)

-- COMMAND ----------


