-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.notebook.run("Ingestion", 0)

-- COMMAND ----------

-- MAGIC %run /Repos/tim.stemke@initions-consulting.com/databricks/Projects/Covid/Utilities

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import databricks.koalas as ks

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
on trim(T.source_country_name) = trim(S.source_country_name)
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
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'UK', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'United Kingdom';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'USA', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'United States';

UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Saint Kitts and Nevis', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Saint Kitts And Nevis';
UPDATE covid_raw.TBL_COUNTRY_MAPPING SET target_country_name = 'Saint Vincent and the Grenadines', UPDATE_TS = CURRENT_TIMESTAMP WHERE source_country_name = 'Saint Vincent And the Grenadines';


-- COMMAND ----------

OPTIMIZE covid_raw.TBL_COUNTRY_MAPPING

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **csse_covid_19_daily_reports**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_csse_covid_19_daily_reports 
AS 
SELECT
FIPS,
Admin2,
Province_State,
Country_Region,
Last_Update,
Lat,
Long_,
Confirmed,
Deaths,
Recovered,
Active,
Combined_Key,
Incident_Rate,
Case_Fatality_Ratio,
file_name,
INPUT_FILE_NAME() AS SOURCE
FROM
covid_ingestion.TBL_csse_covid_19_daily_reports

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_raw.TBL_csse_covid_19_daily_reports (
FIPS STRING,
Admin2 STRING,
Province_State STRING,
Country_Region STRING,
Last_Update STRING,
Lat STRING,
Long_ STRING,
Confirmed STRING,
Deaths STRING,
Recovered STRING,
Active STRING,
Combined_Key STRING,
Incident_Rate STRING,
Case_Fatality_Ratio STRING,
file_name STRING,
SOURCE STRING,
INSERT_TS STRING,
UPDATE_TS STRING
)
USING DELTA LOCATION '/mnt/kaggle/Covid/Raw/csse_covid_19_daily_reports/'

-- COMMAND ----------

MERGE INTO 
covid_raw.TBL_csse_covid_19_daily_reports AS T 
USING 
COVID_RAW.VW_csse_covid_19_daily_reports AS S
ON T.Last_Update = S.Last_Update and T.Combined_Key = S.Combined_Key
WHEN NOT MATCHED
THEN INSERT(
T.FIPS,
T.Admin2,
T.Province_State,
T.Country_Region,
T.Last_Update,
T.Lat,
T.Long_,
T.Confirmed,
T.Deaths,
T.Recovered,
T.Active,
T.Combined_Key,
T.Incident_Rate,
T.Case_Fatality_Ratio,
T.file_name,
T.SOURCE)
VALUES (
S.FIPS,
S.Admin2,
S.Province_State,
S.Country_Region,
S.Last_Update,
S.Lat,
S.Long_,
S.Confirmed,
S.Deaths,
S.Recovered,
S.Active,
S.Combined_Key,
S.Incident_Rate,
S.Case_Fatality_Ratio,
S.file_name,
S.SOURCE)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC def rename_columns(s) -> str:
-- MAGIC   new_column = s.replace(' ','_').replace('/','_').replace('-','_')
-- MAGIC   
-- MAGIC   if 'Long' in s:
-- MAGIC     new_column = 'Longitude'
-- MAGIC   if 'Lat' in s:
-- MAGIC     new_column = 'Latitude'
-- MAGIC   if 'Incid' in s:
-- MAGIC     new_column = 'Incidence_Rate'
-- MAGIC   return new_column
-- MAGIC 
-- MAGIC mount_point = '/mnt/kaggle/Covid/Ingestion/csse_covid_19_daily_reports/'
-- MAGIC 
-- MAGIC file_list = [file.name for file in dbutils.fs.ls("dbfs:{}".format(mount_point))]
-- MAGIC 
-- MAGIC for file in file_list:
-- MAGIC   loadFile = "{0}/{1}".format(mount_point, file)
-- MAGIC   df_csse_covid_19_daily_reports = ks.read_csv(loadFile,dtype=str)
-- MAGIC   
-- MAGIC   df_csse_covid_19_daily_reports['_file_name'] = file
-- MAGIC   
-- MAGIC   df_renamed = df_csse_covid_19_daily_reports.rename(rename_columns, axis='columns')
-- MAGIC   
-- MAGIC   if 'Active' not in df_renamed.columns:
-- MAGIC     df_renamed['Active'] = ''
-- MAGIC    
-- MAGIC   if 'Latitude' not in df_renamed.columns:
-- MAGIC     df_renamed['Latitude'] = ''
-- MAGIC 
-- MAGIC   if 'Longitude' not in df_renamed.columns:
-- MAGIC     df_renamed['Longitude'] = ''
-- MAGIC 
-- MAGIC   if 'FIPS' not in df_renamed.columns:
-- MAGIC     df_renamed['FIPS'] = ''
-- MAGIC 
-- MAGIC   if 'Admin2' not in df_renamed.columns:
-- MAGIC     df_renamed['Admin2'] = ''
-- MAGIC 
-- MAGIC   if 'Combined_Key' not in df_renamed.columns:
-- MAGIC     df_renamed['Combined_Key'] = ''
-- MAGIC 
-- MAGIC   if 'Incidence_Rate' not in df_renamed.columns:
-- MAGIC     df_renamed['Incidence_Rate'] = ''
-- MAGIC 
-- MAGIC   if 'Case_Fatality_Ratio' not in df_renamed.columns:
-- MAGIC     df_renamed['Case_Fatality_Ratio'] = ''
-- MAGIC     
-- MAGIC   df_final = df_renamed[["FIPS","Admin2","Province_State","Country_Region","Last_Update","Latitude","Longitude","Confirmed","Deaths","Recovered","Active","Combined_Key","Incidence_Rate","Case_Fatality_Ratio","_file_name"]]
-- MAGIC   
-- MAGIC   df_final.to_delta(path= '/mnt/kaggle/Covid/Raw/csse_covid_19_daily_reports/',mode='append', index=False)
-- MAGIC   
-- MAGIC   

-- COMMAND ----------

SELECT * FROM delta.`/mnt/kaggle/Covid/Raw/csse_covid_19_daily_reports/`
where Deaths is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_vaccinations_timeseries_v2**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_GERMANY_VACCINATIONS_TIMESERIES_V2 AS
SELECT 
date,
dosen_kumulativ,
dosen_differenz_zum_vortag,
dosen_erst_differenz_zum_vortag,
dosen_zweit_differenz_zum_vortag,
dosen_biontech_kumulativ,
dosen_moderna_kumulativ,
dosen_astrazeneca_kumulativ,
personen_erst_kumulativ,
personen_voll_kumulativ,
impf_quote_erst,
impf_quote_voll,
indikation_alter_dosen,
indikation_beruf_dosen,
indikation_medizinisch_dosen,
indikation_pflegeheim_dosen,
indikation_alter_erst,
indikation_beruf_erst,
indikation_medizinisch_erst,
indikation_pflegeheim_erst,
indikation_alter_voll,
indikation_beruf_voll,
indikation_medizinisch_voll,
indikation_pflegeheim_voll,
INPUT_FILE_NAME() AS SOURCE
FROM
covid_ingestion.TBL_germany_vaccinations_timeseries_v2

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_GERMANY_VACCINATIONS_TIMESERIES_V2 (
date STRING,
dosen_kumulativ STRING,
dosen_differenz_zum_vortag STRING,
dosen_erst_differenz_zum_vortag STRING,
dosen_zweit_differenz_zum_vortag STRING,
dosen_biontech_kumulativ STRING,
dosen_moderna_kumulativ STRING,
dosen_astrazeneca_kumulativ STRING,
personen_erst_kumulativ STRING,
personen_voll_kumulativ STRING,
impf_quote_erst STRING,
impf_quote_voll STRING,
indikation_alter_dosen STRING,
indikation_beruf_dosen STRING,
indikation_medizinisch_dosen STRING,
indikation_pflegeheim_dosen STRING,
indikation_alter_erst STRING,
indikation_beruf_erst STRING,
indikation_medizinisch_erst STRING,
indikation_pflegeheim_erst STRING,
indikation_alter_voll STRING,
indikation_beruf_voll STRING,
indikation_medizinisch_voll STRING,
indikation_pflegeheim_voll STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/germany_vaccinations_timeseries_v2/'

-- COMMAND ----------

MERGE INTO 
COVID_RAW.TBL_GERMANY_VACCINATIONS_TIMESERIES_V2 AS T 
USING 
COVID_RAW.VW_GERMANY_VACCINATIONS_TIMESERIES_V2 AS S
ON
T.DATE = S.DATE
WHEN NOT MATCHED
THEN INSERT (
T.date,
T.dosen_kumulativ,
T.dosen_differenz_zum_vortag,
T.dosen_erst_differenz_zum_vortag,
T.dosen_zweit_differenz_zum_vortag,
T.dosen_biontech_kumulativ,
T.dosen_moderna_kumulativ,
T.dosen_astrazeneca_kumulativ,
T.personen_erst_kumulativ,
T.personen_voll_kumulativ,
T.impf_quote_erst,
T.impf_quote_voll,
T.indikation_alter_dosen,
T.indikation_beruf_dosen,
T.indikation_medizinisch_dosen,
T.indikation_pflegeheim_dosen,
T.indikation_alter_erst,
T.indikation_beruf_erst,
T.indikation_medizinisch_erst,
T.indikation_pflegeheim_erst,
T.indikation_alter_voll,
T.indikation_beruf_voll,
T.indikation_medizinisch_voll,
T.indikation_pflegeheim_voll,
T.SOURCE)
VALUES (
S.date,
S.dosen_kumulativ,
S.dosen_differenz_zum_vortag,
S.dosen_erst_differenz_zum_vortag,
S.dosen_zweit_differenz_zum_vortag,
S.dosen_biontech_kumulativ,
S.dosen_moderna_kumulativ,
S.dosen_astrazeneca_kumulativ,
S.personen_erst_kumulativ,
S.personen_voll_kumulativ,
S.impf_quote_erst,
S.impf_quote_voll,
S.indikation_alter_dosen,
S.indikation_beruf_dosen,
S.indikation_medizinisch_dosen,
S.indikation_pflegeheim_dosen,
S.indikation_alter_erst,
S.indikation_beruf_erst,
S.indikation_medizinisch_erst,
S.indikation_pflegeheim_erst,
S.indikation_alter_voll,
S.indikation_beruf_voll,
S.indikation_medizinisch_voll,
S.indikation_pflegeheim_voll,
S.SOURCE)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_deliveries_timeseries_v2**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_GERMANY_DELIVERIES_TIMESERIES_V2 AS
SELECT 
date,
impfstoff, 
region, 
dosen,
INPUT_FiLE_NAME() AS SOURCE
FROM covid_ingestion.TBL_germany_deliveries_timeseries_v2

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_GERMANY_DELIVERIES_TIMESERIES_V2 (
date STRING,
impfstoff STRING, 
region STRING, 
dosen STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/GERMANY_DELIVERIES_TIMESERIES_V2/'

-- COMMAND ----------

MERGE INTO 
COVID_RAW.TBL_GERMANY_DELIVERIES_TIMESERIES_V2 AS T
USING
COVID_RAW.VW_GERMANY_DELIVERIES_TIMESERIES_V2 AS S
ON
T.DATE = S.DATE
WHEN NOT MATCHED 
THEN INSERT (
T.date,
T.impfstoff, 
T.region, 
T.dosen,
T.SOURCE
)
VALUES (
S.date,
S.impfstoff, 
S.region, 
S.dosen,
S.SOURCE
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_vaccinations_by_state_v1**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_GERMANY_VACCINATIONS_BY_STATE_V1 AS
SELECT
code,
vaccinationsTotal,
peopleFirstTotal,
peopleFullTotal,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_germany_vaccinations_by_state_v1

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_GERMANY_VACCINATIONS_BY_STATE_V1 (
code STRING,
vaccinationsTotal STRING,
peopleFirstTotal STRING,
peopleFullTotal STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/germany_vaccinations_by_state_v1/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_GERMANY_VACCINATIONS_BY_STATE_V1 AS T
USING COVID_RAW.VW_GERMANY_VACCINATIONS_BY_STATE_V1 AS S
ON T.code = S.code
WHEN MATCHED THEN
UPDATE SET
T.vaccinationsTotal = S.vaccinationsTotal,
T.peopleFirstTotal = S.peopleFirstTotal,
T.peopleFullTotal = S.peopleFullTotal,
T.SOURCE = S.SOURCE
WHEN NOT MATCHED THEN INSERT (
T.code,
T.vaccinationsTotal,
T.peopleFirstTotal,
T.peopleFullTotal,
T.SOURCE
)
VALUES (
S.code,
S.vaccinationsTotal,
S.peopleFirstTotal,
S.peopleFullTotal,
S.SOURCE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Altersgruppen**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_RKI_ALTERSGRUPPEN AS
SELECT
_corrupt_record,
geometry,
properties,
type,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_RKI_Altersgruppen

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_ALTERSGRUPPEN (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING,
source STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/RKI_Altersgruppen/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_RKI_ALTERSGRUPPEN AS T
USING COVID_RAW.VW_RKI_ALTERSGRUPPEN AS S
ON T.properties = S.properties --and S.properties is not null
WHEN NOT MATCHED and S.properties is not null THEN
INSERT 
(T.properties, T.source)
VALUES
(S.properties, S.source)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_COVID19**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_RKI_COVID19 AS 
SELECT
_corrupt_record,
geometry,
properties,
type,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_RKI_COVID19

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_COVID19 (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING,
SOURCE STRING)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/RKI_COVID19/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_RKI_COVID19 AS T
USING COVID_RAW.VW_RKI_COVID19 AS S
ON T.properties = S.properties --and S.properties is not null
WHEN NOT MATCHED and S.properties is not null THEN
INSERT 
(T.properties, T.source)
VALUES
(S.properties, S.source)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Corona_Landkreise**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_RKI_CORONA_LANDKREISE AS 
SELECT 
_corrupt_record,
geometry,
properties,
type,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_RKI_CORONA_LANDKREISE

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_CORONA_LANDKREISE (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/RKI_Corona_Landkreise/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_RKI_CORONA_LANDKREISE AS T
USING COVID_RAW.VW_RKI_CORONA_LANDKREISE AS S
ON T.properties = S.properties --and S.properties is not null
WHEN NOT MATCHED and S.properties is not null THEN
INSERT 
(T.properties, T.source)
VALUES
(S.properties, S.source)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Corona_Bundeslaender**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_RKI_CORONA_BUNDESLAENDER AS 
SELECT 
_corrupt_record,
geometry,
properties,
type,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_RKI_CORONA_BUNDESLAENDER

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_CORONA_BUNDESLAENDER (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/RKI_Corona_Bundeslaender/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_RKI_CORONA_BUNDESLAENDER AS T
USING COVID_RAW.VW_RKI_CORONA_BUNDESLAENDER AS S
ON T.properties = S.properties --and S.properties is not null
WHEN NOT MATCHED and S.properties is not null THEN
INSERT 
(T.properties, T.source)
VALUES
(S.properties, S.source)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Data_Status**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_RKI_DATA_STATUS AS 
SELECT 
_corrupt_record,
geometry,
properties,
type,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_RKI_DATA_STATUS

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_DATA_STATUS (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/RKI_Data_Status/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_RKI_DATA_STATUS AS T
USING COVID_RAW.VW_RKI_DATA_STATUS AS S
ON T.properties = S.properties --and S.properties is not null
WHEN NOT MATCHED and S.properties is not null THEN
INSERT 
(T.properties, T.source)
VALUES
(S.properties, S.source)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_key_data**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_RKI_KEY_DATA AS 
SELECT 
_corrupt_record,
geometry,
properties,
type,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_RKI_KEY_DATA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_KEY_DATA (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/RKI_Key_Data/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_RKI_KEY_DATA AS T
USING COVID_RAW.VW_RKI_KEY_DATA AS S
ON T.properties = S.properties --and S.properties is not null
WHEN NOT MATCHED and S.properties is not null THEN
INSERT 
(T.properties, T.source)
VALUES
(S.properties, S.source)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_history**

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_RAW.VW_RKI_HISTORY AS 
SELECT 
_corrupt_record,
geometry,
properties,
type,
INPUT_FILE_NAME() AS SOURCE
FROM covid_ingestion.TBL_RKI_HISTORY

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_HISTORY (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING,
SOURCE STRING
)
USING DELTA
LOCATION '/mnt/kaggle/Covid/Raw/RKI_History/'

-- COMMAND ----------

MERGE INTO COVID_RAW.TBL_RKI_HISTORY AS T
USING COVID_RAW.VW_RKI_HISTORY AS S
ON T.properties = S.properties --and S.properties is not null
WHEN NOT MATCHED and S.properties is not null THEN
INSERT 
(T.properties, T.source)
VALUES
(S.properties, S.source)

-- COMMAND ----------


