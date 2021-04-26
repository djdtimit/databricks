-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Raw into Qualified

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **csse_covid_19_daily_reports**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - FIPS: US only. Federal Information Processing Standards code that uniquely identifies counties within the USA.
-- MAGIC - Admin2: County name. US only.
-- MAGIC - Province_State: Province, state or dependency name.
-- MAGIC - Country_Region: Country, region or sovereignty name. The names of locations included on the Website correspond with the official designations used by the U.S. Department of State.
-- MAGIC - Last Update: MM/DD/YYYY HH:mm:ss (24 hour format, in UTC).
-- MAGIC - Lat and Long_: Dot locations on the dashboard. All points (except for Australia) shown on the map are based on geographic centroids, and are not representative of a specific address, building or any location at a spatial scale finer than a province/state. Australian dots are located at the centroid of the largest city in each state.
-- MAGIC - Confirmed: Counts include confirmed and probable (where reported).
-- MAGIC - Deaths: Counts include confirmed and probable (where reported).
-- MAGIC - Recovered: Recovered cases are estimates based on local media reports, and state and local reporting when available, and therefore may be substantially lower than the true number. US state-level recovered cases are from COVID Tracking Project.
-- MAGIC - Active: Active cases = total cases - total recovered - total deaths.
-- MAGIC - Incident_Rate: Incidence Rate = cases per 100,000 persons.
-- MAGIC - Case_Fatality_Ratio (%): Case-Fatality Ratio (%) = Number recorded deaths / Number cases.
-- MAGIC - All cases, deaths, and recoveries reported are based on the date of initial report. Exceptions to this are noted in the "Data Modification" and "Retrospective reporting of (probable) cases and deaths" subsections below.

-- COMMAND ----------

MERGE INTO covid_qualified.TBL_csse_covid_19_daily_reports T USING covid_qualified.VW_csse_covid_19_daily_reports S ON S.COUNTRY_REGION = T.COUNTRY_REGION
AND S._SOURCE = T._SOURCE
WHEN MATCHED
AND S.Admin2 = T.Admin2
AND S.Province_State = T.Province_State
AND S.LAST_UPDATE > T.LAST_UPDATE THEN
UPDATE
SET
  *
  WHEN NOT MATCHED THEN
INSERT
  *

-- COMMAND ----------

SELECT * FROM covid_qualified.VW_csse_covid_19_daily_reports
WHERE PROVINCE_STATE IS NOT NULL

-- COMMAND ----------


