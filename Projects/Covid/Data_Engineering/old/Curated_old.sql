-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.notebook.run("Qualified", 0)

-- COMMAND ----------

CREATE DATABASE if not exists COVID_CURATED

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Covid_Cases_Vaccinations**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_CURATED.VW_COVID_CASES_VACCINATIONS
AS
SELECT 
    d.date,
    d.country,
    s.continent,
    s.population,
    cumulative_total_cases,
    daily_new_cases,
    d.active_cases,
    cumulative_total_deaths,
    daily_new_deaths,
    iso_code,
    total_vaccinations,
    people_vaccinated,
    people_fully_vaccinated,
    daily_vaccinations_raw,
    daily_vaccinations,
    total_vaccinations_per_hundred,
    people_vaccinated_per_hundred,
    people_fully_vaccinated_per_hundred,
    daily_vaccinations_per_million,
    vaccines
FROM covid_qualified.TBL_worldometer_coronavirus_daily_data D
    LEFT JOIN covid_qualified.TBL_country_vaccinations V 
    ON D.COUNTRY = V.COUNTRY
    AND D.DATE = V.DATE
    LEFT JOIN covid_qualified.TBL_worldometer_coronavirus_summary_data S
    ON D.COUNTRY = S.COUNTRY

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_CURATED.TBL_COVID_CASES_VACCINATIONS (
 date date,
    country string,
    continent string,
    population string,
    cumulative_total_cases int,
    daily_new_cases int,
    active_cases int,
    cumulative_total_deaths int,
    daily_new_deaths int,
    iso_code String,
    total_vaccinations int,
    people_vaccinated int,
    people_fully_vaccinated int,
    daily_vaccinations_raw int,
    daily_vaccinations int,
    total_vaccinations_per_hundred decimal(23,5),
    people_vaccinated_per_hundred decimal(23,5),
    people_fully_vaccinated_per_hundred decimal(23,5),
    daily_vaccinations_per_million decimal(23,5),
    vaccines STRING,
    INSERT_TS TIMESTAMP,
    UPDATE_TS TIMESTAMP
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Curated/Covid_Cases_Vaccinations/'

-- COMMAND ----------

MERGE INTO COVID_CURATED.TBL_COVID_CASES_VACCINATIONS T
USING COVID_CURATED.VW_COVID_CASES_VACCINATIONS S
ON T.date = S.DATE and T.COUNTRY = S.COUNTRY 
WHEN MATCHED THEN
UPDATE SET 
T.continent = S.continent,
T.population = S.population,
T.cumulative_total_cases = S.cumulative_total_cases,
T.daily_new_cases = S.daily_new_cases,
T.active_cases = S.active_cases,
T.cumulative_total_deaths = S.cumulative_total_deaths,
T.daily_new_deaths = S.daily_new_deaths,
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
T.ISO_CODE = S.ISO_CODE,
T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT
(T.date,
T.country,
T.continent,
T.population,
T.cumulative_total_cases,
T.daily_new_cases,
T.active_cases,
T.cumulative_total_deaths,
T.daily_new_deaths,
T.iso_code,
T.total_vaccinations,
T.people_vaccinated,
T.people_fully_vaccinated,
T.daily_vaccinations_raw,
T.daily_vaccinations,
T.total_vaccinations_per_hundred,
T.people_vaccinated_per_hundred,
T.people_fully_vaccinated_per_hundred,
T.daily_vaccinations_per_million,
T.vaccines,
T.INSERT_TS,
T.UPDATE_TS
)
VALUES
(S.date,
S.country,
S.continent,
S.population,
S.cumulative_total_cases,
S.daily_new_cases,
S.active_cases,
S.cumulative_total_deaths,
S.daily_new_deaths,
S.iso_code,
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
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Covid_DE**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_CURATED.VW_COVID_DE
AS
SELECT 
    c.state,
    c.country,
    c.age_group,
    c.gender,
    c.date,
    c.cases,
    c.death,
    c.recovered,
    d.population
FROM covid_qualified.TBL_covid_de c
left join
covid_qualified.TBL_demographics_DE d
on
c.state = d.state and c.gender = d.gender and c.age_group = d.age_group

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_CURATED.TBL_COVID_DE (
state STRING,
country STRING,
age_group STRING,
gender STRING,
date DATE,
cases INTEGER,
death INTEGER,
recovered INTEGER,
population INTEGER,
INSERT_TS TIMESTAMP,
UPDATE_TS TIMESTAMP
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Curated/Covid_DE/'

-- COMMAND ----------

MERGE INTO COVID_CURATED.TBL_COVID_DE T
USING COVID_CURATED.VW_COVID_DE S
ON T.state = S.state and T.country = S.country AND T.age_group = S.age_group and T.gender = S.gender and T.date = s.date
WHEN MATCHED THEN
UPDATE SET T.CASES = S.CASES, T.DEATH = S.DEATH, T.RECOVERED = S.RECOVERED, T.POPULATION = S.POPULATION, T.UPDATE_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT (T.state, T.country, T.age_group, T.gender, T.date, T.cases, T.death, T.recovered, T.population, T.INSERT_TS, T.UPDATE_TS)
VALUES
(S.state, S.country, S.age_group, S.gender, S.date, S.cases, S.death, S.recovered, S.population, current_timestamp, current_timestamp)
