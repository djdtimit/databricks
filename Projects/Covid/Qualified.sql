-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.notebook.run("Raw", 0)

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

-- MAGIC %md
-- MAGIC **Worldometer_coronavirus_daily_data**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_qualified.worldometer_coronavirus_daily_data as
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

-- MAGIC %md
-- MAGIC **worldometer_coronavirus_summary_data**

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_qualified.worldometer_coronavirus_summary_data AS
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


