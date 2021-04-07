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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **csse_covid_19_daily_reports**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_csse_covid_19_daily_reports (
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
file_name STRING
)
USING CSV
OPTIONS ("header" True)
LOCATION "/mnt/kaggle/Covid/Ingestion/csse_covid_19_daily_reports/*.csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_vaccinations_timeseries_v2**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_germany_vaccinations_timeseries_v2 (
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
indikation_pflegeheim_voll STRING

)
USING CSV
OPTIONS ("header" True, "sep" ";")
LOCATION "/mnt/kaggle/Covid/Ingestion/germany_vaccinations_timeseries_v2/*.csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_deliveries_timeseries_v2**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_germany_deliveries_timeseries_v2 (
date STRING,
impfstoff STRING, 
region STRING, 
dosen STRING
)
USING CSV
OPTIONS ("header" True, "sep" ";")
LOCATION 
'/mnt/kaggle/Covid/Ingestion/germany_deliveries_timeseries_v2/*.csv'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_vaccinations_by_state_v1**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_germany_vaccinations_by_state_v1 (
code STRING,
vaccinationsTotal STRING,
peopleFirstTotal STRING,
peopleFullTotal STRING
)
USING CSV
OPTIONS ("header" True, "sep" ";")
LOCATION 
'/mnt/kaggle/Covid/Ingestion/germany_vaccinations_by_state_v1/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Altersgruppen**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_RKI_Altersgruppen (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING
)
USING json
LOCATION 
'/mnt/kaggle/Covid/Ingestion/RKI_Altersgruppen/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_COVID19**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_RKI_COVID19 (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING
)
USING json
LOCATION 
'/mnt/kaggle/Covid/Ingestion/RKI_COVID19/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Corona_Landkreise**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_RKI_Corona_Landkreise (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING
)
USING json
LOCATION 
'/mnt/kaggle/Covid/Ingestion/RKI_Corona_Landkreise/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Corona_Bundeslaender**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_RKI_Corona_Bundeslaender (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING
)
USING json
LOCATION 
'/mnt/kaggle/Covid/Ingestion/RKI_Corona_Bundeslaender/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Data_Status**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_RKI_Data_Status (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING
)
USING json
LOCATION 
'/mnt/kaggle/Covid/Ingestion/RKI_Data_Status/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_key_data**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_RKI_key_data (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING
)
USING json
LOCATION 
'/mnt/kaggle/Covid/Ingestion/RKI_key_data/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_history**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_ingestion.TBL_RKI_history (
_corrupt_record STRING,
geometry STRING,
properties STRING,
type STRING
)
USING json
LOCATION 
'/mnt/kaggle/Covid/Ingestion/RKI_history/'
