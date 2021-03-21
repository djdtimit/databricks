-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.notebook.run("Ingestion", 0)

-- COMMAND ----------

-- MAGIC %run /Repos/tim.stemke@initions-consulting.com/databricks/Projects/Covid/Utilities

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS covid_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Country Vaccinations**

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

OPTIMIZE covid_raw.TBL_country_vaccinations

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

OPTIMIZE covid_raw.tbl_covid_de

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

OPTIMIZE covid_raw.TBL_DEMOGRAPHICS_DE

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

OPTIMIZE covid_raw.TBL_Worldometer_coronavirus_daily_data

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

OPTIMIZE covid_raw.TBL_worldometer_coronavirus_summary_data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dl = data_lake(ingestion_schema = 'covid_ingestion', raw_schema = 'covid_raw')
-- MAGIC dl.mv_ingestion_to_archive()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **country_mapping_table**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_raw.TBL_COUNTRY_MAPPING (
TARGET_COUNTRY_NAME STRING,
SOURCE_COUNTRY_NAME STRING,
INSERT_TS STRING,
UPDATE_TS STRING
)
USING DELTA LOCATION '/mnt/kaggle/Covid/Raw/country_mapping/'

-- COMMAND ----------

MERGE INTO covid_raw.TBL_COUNTRY_MAPPING AS T
USING 
(SELECT trim(country) as target_country_name, trim(country) as source_country_name, current_timestamp as INSERT_TS, current_timestamp as UPDATE_TS FROM (
SELECT distinct trim(country) as country FROM covid_raw.TBL_country_vaccinations
UNION
SELECT distinct trim(country) as country FROM covid_raw.tbl_worldometer_coronavirus_summary_data)) S
on UPPER(trim(T.source_country_name)) = UPPER(trim(S.source_country_name))
WHEN NOT MATCHED
THEN INSERT ( target_Country_name, source_Country_name, insert_ts, update_ts) VALUES (S.target_country_name, S.source_country_name, current_timestamp, current_Timestamp)


-- COMMAND ----------

UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Antigua and Barbuda', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Antigua And Barbuda';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Hong Kong', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'China Hong Kong Sar';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Macao', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'China Macao Sar';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Cote d\'Ivoire', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Cote D Ivoire';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Czech Republic', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Czechia';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Falkland Islands', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Falkland Islands Malvinas';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Isle of Man', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Isle Of Man';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Trinidad and Tobago', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Trinidad And Tobago';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Turks and Caicos Islands', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Turks And Caicos Islands';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Vietnam', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Viet Nam';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Cyprus', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Nothern Cyprus';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'UK', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'United Kingdom';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'USA', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'United States';

-- COMMAND ----------

OPTIMIZE covid_raw.TBL_COUNTRY_MAPPING

-- COMMAND ----------


