-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.notebook.run("Raw", 0)

-- COMMAND ----------

CREATE WIDGET TEXT update_duration_days DEFAULT "10"


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS covid_qualified

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Country Vaccinations**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_qualified.VW_country_vaccinations AS
SELECT 
M.TARGET_COUNTRY_NAME AS COUNTRY
,V.ISO_CODE
,CAST (date as DATE)
,CAST (total_vaccinations as INTEGER)
,CAST (people_vaccinated as INTEGER)
,CAST (people_fully_vaccinated AS INTEGER)
,CAST (daily_vaccinations_raw AS INTEGER)
,CAST(daily_vaccinations AS INTEGER)
,CAST(total_vaccinations_per_hundred as DECIMAL(23,5))
,CAST(people_vaccinated_per_hundred as DECIMAL(23,5))
,CAST(people_fully_vaccinated_per_hundred as DECIMAL(23,5))
,CAST(daily_vaccinations_per_million as DECIMAL(23,5))
,CAST(vaccines as STRING)
,CAST(source_name as STRING)
,CAST(source_website AS STRING)
FROM covid_raw.TBL_country_vaccinations V
JOIN covid_raw.TBL_COUNTRY_MAPPING M
ON
V.COUNTRY = M.SOURCE_COUNTRY_NAME


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_country_vaccinations (
COUNTRY STRING
,ISO_CODE STRING
,date DATE
,total_vaccinations INTEGER
,people_vaccinated INTEGER
,people_fully_vaccinated INTEGER
,daily_vaccinations_raw INTEGER
,daily_vaccinations INTEGER
,total_vaccinations_per_hundred DECIMAL(23,5)
,people_vaccinated_per_hundred DECIMAL(23,5)
,people_fully_vaccinated_per_hundred DECIMAL(23,5)
,daily_vaccinations_per_million DECIMAL(23,5)
,vaccines STRING
,source_name STRING
,source_website STRING
,INSERT_TS TIMESTAMP
,UPDATE_TS TIMESTAMP
)
USING DELTA
LOCATION "/mnt/kaggle/Covid/Qualified/covid_19_world_vaccination_progress/country_vaccinations/"

-- COMMAND ----------

MERGE INTO covid_qualified.TBL_country_vaccinations T
USING 
covid_qualified.VW_country_vaccinations S
ON T.COUNTRY = S.COUNTRY AND T.DATE = S.DATE AND T.ISO_CODE = S.ISO_CODE
WHEN MATCHED and datediff(current_date, date(T.UPDATE_TS)) >= getArgument("update_duration_days")
-- data cleaning
AND S.ISO_CODE IS NOT NULL
THEN
UPDATE SET
T.ISO_CODE = S.ISO_CODE
,T.total_vaccinations = S.total_vaccinations
,T.people_vaccinated = S.people_vaccinated
,T.people_fully_vaccinated = S.people_fully_vaccinated
,T.daily_vaccinations_raw = S.daily_vaccinations_raw
,T.daily_vaccinations = S.daily_vaccinations
,T.total_vaccinations_per_hundred = S.total_vaccinations_per_hundred
,T.people_vaccinated_per_hundred = S.people_vaccinated_per_hundred
,T.people_fully_vaccinated_per_hundred = S.people_fully_vaccinated_per_hundred
,T.daily_vaccinations_per_million = S.daily_vaccinations_per_million
,T.vaccines = S.vaccines
,T.source_name = S.source_name
,T.source_website = S.source_website
,T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED 
-- Data Cleaning
AND S.ISO_CODE IS NOT NULL 
THEN INSERT
(
T.COUNTRY
,T.ISO_CODE
,T.date
,T.total_vaccinations
,T.people_vaccinated
,T.people_fully_vaccinated
,T.daily_vaccinations_raw
,T.daily_vaccinations
,T.total_vaccinations_per_hundred
,T.people_vaccinated_per_hundred
,T.people_fully_vaccinated_per_hundred
,T.daily_vaccinations_per_million
,T.vaccines
,T.source_name
,T.source_website
,T.INSERT_TS
,T.UPDATE_TS
)
VALUES (
S.COUNTRY
,S.ISO_CODE
,S.date
,S.total_vaccinations
,S.people_vaccinated
,S.people_fully_vaccinated
,S.daily_vaccinations_raw
,S.daily_vaccinations
,S.total_vaccinations_per_hundred
,S.people_vaccinated_per_hundred
,S.people_fully_vaccinated_per_hundred
,S.daily_vaccinations_per_million
,S.vaccines
,S.source_name
,S.source_website
,CURRENT_TIMESTAMP
,CURRENT_TIMESTAMP)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Covid_DE**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_qualified.VW_covid_de AS
SELECT 
state,
country,
age_group,
gender,
cast(date as date),
cast(cases as INTEGER),
cast(death as INTEGER),
cast(recovered as INTEGER)
FROM covid_raw.TBL_COVID_DE

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_covid_de (
state STRING,
country STRING,
age_group STRING,
gender STRING,
date DATE,
CASES INTEGER,
DEATH INTEGER,
RECOVERED INTEGER
,INSERT_TS TIMESTAMP
,UPDATE_TS TIMESTAMP)
USING DELTA
LOCATION "/mnt/kaggle/Covid/Qualified/covid19-tracking-germany/Covid_DE/"

-- COMMAND ----------

MERGE INTO covid_qualified.TBL_covid_de T 
USING
covid_qualified.VW_covid_de S
ON T.state = S.state and T.country = S.country AND T.age_group = S.age_group and T.gender = S.gender and T.date = s.date
WHEN MATCHED and datediff(current_date, date(T.UPDATE_TS)) >= getArgument("update_duration_days") 
THEN
UPDATE SET T.CASES = S.CASES, T.DEATH = S.DEATH, T.RECOVERED = S.RECOVERED, T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT 
(T.state
,T.country
,T.age_group
,T.gender
,T.date
,T.CASES
,T.DEATH
,T.RECOVERED
,T.INSERT_TS
,T.UPDATE_TS
)
VALUES
(S.state
,S.country
,S.age_group
,S.gender
,S.date
,S.CASES
,S.DEATH
,S.RECOVERED
,CURRENT_TIMESTAMP
,CURRENT_TIMESTAMP)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Demographics_DE**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_qualified.VW_demographics_de as 
SELECT state,
gender,
age_group,
cast(population as INTEGER) 
FROM covid_raw.TBL_DEMOGRAPHICS_DE

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_demographics_DE (
state STRING,
GENDER STRING,
AGE_GROUP STRING,
POPULATION INTEGER
,INSERT_TS TIMESTAMP
,UPDATE_TS TIMESTAMP)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Qualified/covid19-tracking-germany/Demographics_DE/'

-- COMMAND ----------

MERGE INTO covid_qualified.TBL_demographics_DE T
USING
covid_qualified.VW_demographics_de S
ON T.state = S.state and T.gender = S.gender and T.age_group = S.age_group
WHEN MATCHED and datediff(current_date, date(T.UPDATE_TS)) >= getArgument("update_duration_days")
THEN 
UPDATE SET T.POPULATION = S.POPULATION, T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT (T.STATE, T.GENDER, T.AGE_GROUP, T.POPULATION, T.INSERT_TS, T.UPDATE_TS) 
VALUES (S.STATE, S.GENDER, S.AGE_GROUP, S.POPULATION, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Worldometer_coronavirus_daily_data**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_qualified.VW_worldometer_coronavirus_daily_data as
SELECT 
cast(date as date),
target_country_name as country,
CAST(CUMULATIVE_TOTAL_CASES AS INTEGER),
CAST(DAILY_NEW_CASES AS INTEGER),
CAST(ACTIVE_CASES AS INTEGER),
CAST(CUMULATIVE_TOTAL_DEATHS AS INTEGER),
CAST(DAILY_NEW_DEATHS AS INTEGER)
FROM covid_raw.TBL_Worldometer_coronavirus_daily_data D
JOIN covid_raw.TBL_COUNTRY_MAPPING M
ON
D.COUNTRY = M.SOURCE_COUNTRY_NAME

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_worldometer_coronavirus_daily_data (
date date,
country STRING,
CUMULATIVE_TOTAL_CASES INTEGER,
DAILY_NEW_CASES INTEGER,
ACTIVE_CASES INTEGER,
CUMULATIVE_TOTAL_DEATHS INTEGER,
DAILY_NEW_DEATHS INTEGER
,INSERT_TS TIMESTAMP
,UPDATE_TS TIMESTAMP
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Qualified/covid19-global-dataset/worldometer_coronavirus_daily_data/'

-- COMMAND ----------

MERGE INTO covid_qualified.TBL_worldometer_coronavirus_daily_data T
USING covid_qualified.VW_worldometer_coronavirus_daily_data S
ON T.DATE = S.DATE AND T.COUNTRY = S.COUNTRY
WHEN MATCHED and datediff(current_date, date(T.UPDATE_TS)) >= getArgument("update_duration_days")
THEN
UPDATE SET 
T.CUMULATIVE_TOTAL_CASES = S.CUMULATIVE_TOTAL_CASES,
T.DAILY_NEW_CASES = S.DAILY_NEW_CASES,
T.ACTIVE_CASES = S.ACTIVE_CASES,
T.CUMULATIVE_TOTAL_DEATHS = S.CUMULATIVE_TOTAL_DEATHS,
T.DAILY_NEW_DEATHS = S.DAILY_NEW_DEATHS,
T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT (
T.date,
T.country,
T.CUMULATIVE_TOTAL_CASES,
T.DAILY_NEW_CASES,
T.ACTIVE_CASES,
T.CUMULATIVE_TOTAL_DEATHS,
T.DAILY_NEW_DEATHS,
T.INSERT_TS,
T.UPDATE_TS
)
VALUES (
S.date,
S.country,
S.CUMULATIVE_TOTAL_CASES,
S.DAILY_NEW_CASES,
S.ACTIVE_CASES,
S.CUMULATIVE_TOTAL_DEATHS,
S.DAILY_NEW_DEATHS,
current_timestamp,
current_timestamp)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **worldometer_coronavirus_summary_data**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_qualified.VW_worldometer_coronavirus_summary_data AS
SELECT 
target_country_name as country,
continent,
cast(total_confirmed as INTEGER),
cast(total_deaths as INTEGER),
cast(total_recovered as INTEGER),
cast(active_cases as INTEGER),
cast(Serious_or_critical as INTEGER),
cast(total_cases_per_lm_population as DECIMAL(23,5)),
cast(total_tests as INTEGER),
cast(total_tests_per_lm_population as DECIMAL(23,5)),
cast(population as INTEGER)
FROM
covid_raw.TBL_worldometer_coronavirus_summary_data s
join covid_raw.TBL_COUNTRY_MAPPING M
ON
s.COUNTRY = M.SOURCE_COUNTRY_NAME

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_worldometer_coronavirus_summary_data (
country STRING,
continent STRING,
total_confirmed INTEGER,
total_deaths INTEGER,
total_recovered INTEGER,
active_cases INTEGER,
Serious_or_critical INTEGER,
total_cases_per_lm_population DECIMAL(23,5),
total_tests INTEGER,
total_tests_per_lm_population DECIMAL(23,5),
population INTEGER
,INSERT_TS TIMESTAMP
,UPDATE_TS TIMESTAMP
)
USING DELTA
LOCATION "/mnt/kaggle/Covid/Qualified/covid19-global-dataset/worldometer_coronavirus_summary_data/"

-- COMMAND ----------

MERGE INTO covid_qualified.TBL_worldometer_coronavirus_summary_data T
USING
covid_qualified.VW_worldometer_coronavirus_summary_data S
ON T.country = S.country and T.continent = S.continent
WHEN MATCHED AND datediff(current_date, date(T.UPDATE_TS)) >= getArgument("update_duration_days")
THEN
UPDATE SET 
T.total_confirmed = S.total_confirmed,
T.total_deaths = S.total_deaths,
T.total_recovered = S.total_recovered,
T.active_cases = S.active_cases,
T.Serious_or_critical = S.Serious_or_critical,
T.total_cases_per_lm_population = S.total_cases_per_lm_population,
T.total_tests = S.total_tests,
T.total_tests_per_lm_population = S.total_tests_per_lm_population,
T.population = S.population,
T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT (T.country,
T.continent,
T.total_confirmed,
T.total_deaths,
T.total_recovered,
T.active_cases,
T.Serious_or_critical,
T.total_cases_per_lm_population,
T.total_tests,
T.total_tests_per_lm_population,
T.population,
T.INSERT_TS,
T.UPDATE_TS)
VALUES (
S.country,
S.continent,
S.total_confirmed,
S.total_deaths,
S.total_recovered,
S.active_cases,
S.Serious_or_critical,
S.total_cases_per_lm_population,
S.total_tests,
S.total_tests_per_lm_population,
S.population,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP)
