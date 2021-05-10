-- Databricks notebook source
SELECT Country_REgion, Active, Upper(alpha3) as alpha3 FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE last_update = '2021-05-10T04:20:38.000+0000'

-- COMMAND ----------


