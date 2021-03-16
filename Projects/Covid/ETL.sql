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
-- MAGIC from datetime import date
-- MAGIC 
-- MAGIC class data_lake:
-- MAGIC   
-- MAGIC   def __init__(self, archive_schema = '', ingestion_schema = '', raw_schema = '', qualified_schema = '', curated_schema = ''):
-- MAGIC     self.archive_schema = archive_schema
-- MAGIC     self.ingestion_schema = ingestion_schema
-- MAGIC     self.raw_schema = raw_schema
-- MAGIC     self.qualified_schema = qualified_schema
-- MAGIC     self.curated_schema = curated_schema
-- MAGIC     
-- MAGIC     if self.archive_schema != '':
-- MAGIC       self.archive_views = spark.sql(f"show views in {self.archive_schema}").select('namespace', 'viewName').collect()
-- MAGIC     if self.ingestion_schema != '':
-- MAGIC       self.ingestion_views = spark.sql(f"show views in {self.ingestion_schema}").select('namespace', 'viewName').collect()
-- MAGIC     if self.raw_schema != '':
-- MAGIC       self.raw_views = spark.sql(f"show views in {self.raw_schema}").select('namespace', 'viewName').collect()
-- MAGIC     if self.qualified_schema != '':
-- MAGIC       self.qualified_views = spark.sql(f"show views in {self.qualified_schema}").select('namespace', 'viewName').collect()
-- MAGIC     if self.curated_schema != '':
-- MAGIC       self.curated_views = spark.sql(f"show views in {self.curated_schema}").select('namespace', 'viewName').collect()
-- MAGIC     
-- MAGIC   def mv_ingestion_to_archive(self):
-- MAGIC     for raw_view in self.raw_views:
-- MAGIC       df_source = self.__read_source_table(raw_view)
-- MAGIC       self.__mv_files(df_source)
-- MAGIC       
-- MAGIC   def __read_source_table(self, view_name):
-- MAGIC     df_source = spark.read.table(f"""{view_name['namespace']}.{view_name['viewName']}""").select('source').distinct().collect()
-- MAGIC     return df_source
-- MAGIC   
-- MAGIC   def __mv_files(self, df_source):
-- MAGIC     today = date.today()
-- MAGIC     dbutils.fs.mv(df_source[0]['source'], df_source[0]['source'].replace('/Ingestion/', f'/Archive/{today}/'), recurse=True)

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

CREATE DATABASE IF NOT EXISTS COVID_INGESTION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestion

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
    source_website STRING,
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Raw

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS covid_raw

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_raw.VW_country_vaccinations
AS SELECT 
country,
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
source_website,
INPUT_FILE_NAME() as source
FROM covid_ingestion.TBL_country_vaccinations

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_raw.TBL_country_vaccinations (
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
    UPDATE_TS STRING,
    SOURCE STRING
) USING DELTA
LOCATION "/mnt/kaggle/Covid/Raw/covid_19_world_vaccination_progress/country_vaccinations/"

-- COMMAND ----------

MERGE INTO covid_raw.TBL_country_vaccinations AS T
USING 
covid_raw.VW_country_vaccinations S
on 
T.country = S.country AND
T.iso_code = S.iso_code AND
T.date = S.date 

WHEN MATCHED THEN 
UPDATE SET 
T.total_vaccinations = S.total_vaccinations,
T.people_vaccinated = S.people_vaccinated,
T.people_fully_vaccinated = S.people_fully_vaccinated,
T.daily_vaccinations_raw = S.daily_vaccinations_raw,
T.daily_vaccinations = S.daily_vaccinations,
T.total_vaccinations_per_hundred = S.total_vaccinations_per_hundred,
T.people_vaccinated_per_hundred = S.people_vaccinated_per_hundred,
T.people_fully_vaccinated_per_hundred = S.people_fully_vaccinated_per_hundred,
T.daily_vaccinations_per_million = S.daily_vaccinations_per_million,
T.vaccines = S.vaccines,
T.source_name = S.source_name,
T.source_website = S.source_website,
T.UPDATE_TS = current_timestamp,
T.source = S.source

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
source_website, insert_ts, update_ts, source ) 
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
current_timestamp, current_Timestamp, source)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Covid_DE**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_raw.VW_COVID_DE 
AS
SELECT 
state,
country,
age_group,
gender,
date,
cases,
death,
recovered,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_COVID_DE

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_raw.TBL_COVID_DE (
state STRING,
country STRING,
age_group STRING,
gender STRING,
date STRING,
cases STRING,
death STRING,
recovered STRING,
INSERT_TS STRING,
UPDATE_TS STRING,
SOURCE STRING) 
USING DELTA
LOCATION "/mnt/kaggle/Covid/Raw/covid19-tracking-germany/Covid_DE/"

-- COMMAND ----------

MERGE INTO covid_raw.tbl_covid_de AS T
USING covid_raw.vw_covid_de AS S
ON 
T.state = S.state 
and T.country = S.country 
and T.age_group = S.age_group
AND T.gender = S.gender 
AND T.date = S.date
WHEN MATCHED THEN UPDATE SET
T.cases = S.cases, 
T.death = S.death,
T.recovered = S.recovered,
T.UPDATE_TS = CURRENT_TIMESTAMP(),
T.SOURCE = S.SOURCE
WHEN NOT MATCHED THEN INSERT
(state,
country,
age_group,
gender,
date,
cases,
death,
recovered,
INSERT_TS,
UPDATE_TS,
SOURCE)
VALUES (
state,
country,
age_group,
gender,
date,
cases,
death,
recovered,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP(),
SOURCE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Demographics_DE**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_raw.VW_DEMOGRAPHICS_DE
AS SELECT 
STATE,
GENDER,
AGE_GROUP,
POPULATION,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_DEMOGRAPHICS_DE

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_raw.TBL_DEMOGRAPHICS_DE (
STATE STRING,
GENDER STRING,
AGE_GROUP STRING,
POPULATION STRING,
INSERT_TS STRING,
UPDATE_TS STRING,
SOURCE STRING)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/covid19-tracking-germany/Demographics_DE/'

-- COMMAND ----------

MERGE INTO covid_raw.TBL_DEMOGRAPHICS_DE as T
USING covid_raw.VW_DEMOGRAPHICS_DE as S
ON T.State = S.State and T.Gender = S.Gender and T.Age_Group = S.Age_Group
WHEN MATCHED THEN
UPDATE SET T.POPULATION = S.POPULATION, T.SOURCE = S.SOURCE, T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT
(STATE, GENDER, AGE_GROUP, POPULATION, INSERT_TS, UPDATE_TS, SOURCE)
VALUES (STATE, GENDER, AGE_GROUP, POPULATION, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, SOURCE)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Worldometer_coronavirus_daily_data**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_raw.VW_WORLDOMETER_CORONAVIRUS_DAILY_DATA
AS SELECT
DATE,
COUNTRY,
CUMULATIVE_TOTAL_CASES,
DAILY_NEW_CASES,
ACTIVE_CASES,
CUMULATIVE_TOTAL_DEATHS,
DAILY_NEW_DEATHS,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_Worldometer_coronavirus_daily_data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_raw.TBL_Worldometer_coronavirus_daily_data (
DATE STRING,
COUNTRY STRING,
CUMULATIVE_TOTAL_CASES STRING,
DAILY_NEW_CASES STRING,
ACTIVE_CASES STRING,
CUMULATIVE_TOTAL_DEATHS STRING,
DAILY_NEW_DEATHS STRING,
INSERT_TS STRING,
UPDATE_TS STRING,
SOURCE STRING)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/covid19-global-dataset/worldometer_coronavirus_daily_data/'

-- COMMAND ----------

MERGE INTO covid_raw.TBL_Worldometer_coronavirus_daily_data as T
USING covid_raw.VW_WORLDOMETER_CORONAVIRUS_DAILY_DATA as S
ON T.DATE = S.DATE AND T.COUNTRY = S.COUNTRY
WHEN MATCHED THEN
UPDATE SET
T.CUMULATIVE_TOTAL_CASES = S.CUMULATIVE_TOTAL_CASES, T.DAILY_NEW_CASES = S.DAILY_NEW_CASES
,T.CUMULATIVE_TOTAL_DEATHS = S.CUMULATIVE_TOTAL_DEATHS, T.DAILY_NEW_DEATHS = S.DAILY_NEW_DEATHS, T.SOURCE = S.SOURCE, T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT
(DATE, COUNTRY, CUMULATIVE_TOTAL_CASES, DAILY_NEW_CASES, ACTIVE_CASES, CUMULATIVE_TOTAL_DEATHS, DAILY_NEW_DEATHS, INSERT_TS, UPDATE_TS, SOURCE)
VALUES 
(DATE, COUNTRY, CUMULATIVE_TOTAL_CASES, DAILY_NEW_CASES, ACTIVE_CASES, CUMULATIVE_TOTAL_DEATHS, DAILY_NEW_DEATHS, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, SOURCE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **worldometer_coronavirus_summary_data**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_worldometer_coronavirus_summary_data
AS
SELECT 
country ,
CONTINENT ,
TOTAL_CONFIRMED ,
TOTAL_DEATHS ,
TOTAL_RECOVERED ,
ACTIVE_CASES ,
SERIOUS_OR_CRITICAL ,
TOTAL_CASES_PER_LM_POPULATION ,
TOTAL_TESTS ,
TOTAL_TESTS_PER_LM_POPULATION ,
POPULATION ,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_worldometer_coronavirus_summary_data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_raw.TBL_worldometer_coronavirus_summary_data (
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
POPULATION STRING,
INSERT_TS STRING,
UPDATE_TS STRING,
SOURCE STRING)
USING DELTA
LOCATION "/mnt/kaggle/Covid/Raw/covid19-global-dataset/worldometer_coronavirus_summary_data/"

-- COMMAND ----------

MERGE INTO covid_raw.TBL_worldometer_coronavirus_summary_data AS T
USING COVID_RAW.VW_worldometer_coronavirus_summary_data AS S
ON T.country = S.country and T.continent = S.continent
WHEN MATCHED THEN UPDATE SET
T.TOTAL_CONFIRMED = S.TOTAL_CONFIRMED, T.TOTAL_DEATHS = S.TOTAL_DEATHS, T.TOTAL_RECOVERED = S.TOTAL_RECOVERED, T.ACTIVE_CASES = S.ACTIVE_CASES,
T.SERIOUS_OR_CRITICAL = S.SERIOUS_OR_CRITICAL, T.TOTAL_CASES_PER_LM_POPULATION = S.TOTAL_CASES_PER_LM_POPULATION, T.TOTAL_TESTS = S.TOTAL_TESTS,
T.TOTAL_TESTS_PER_LM_POPULATION = S.TOTAL_TESTS_PER_LM_POPULATION, T.POPULATION = S.POPULATION, T.SOURCE = S.SOURCE, T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT
(country,
CONTINENT,
TOTAL_CONFIRMED,
TOTAL_DEATHS,
TOTAL_RECOVERED,
ACTIVE_CASES,
SERIOUS_OR_CRITICAL,
TOTAL_CASES_PER_LM_POPULATION,
TOTAL_TESTS,
TOTAL_TESTS_PER_LM_POPULATION,
POPULATION,
INSERT_TS,
UPDATE_TS,
SOURCE)
VALUES
(country,
CONTINENT,
TOTAL_CONFIRMED,
TOTAL_DEATHS,
TOTAL_RECOVERED,
ACTIVE_CASES,
SERIOUS_OR_CRITICAL,
TOTAL_CASES_PER_LM_POPULATION,
TOTAL_TESTS,
TOTAL_TESTS_PER_LM_POPULATION,
POPULATION,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP,
SOURCE)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dl = data_lake(ingestion_schema = 'covid_ingestion', raw_schema = 'covid_raw')
-- MAGIC dl.mv_ingestion_to_archive()

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


