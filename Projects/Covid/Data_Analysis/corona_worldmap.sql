-- Databricks notebook source
SELECT Country_Region, Upper(alpha3) as alpha3, sum(Confirmed) AS Confirmed FROM covid_curated.VW_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.VW_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by Confirmed DESC

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Confirmed) AS Confirmed FROM covid_curated.VW_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.VW_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

SELECT sum(Confirmed)
FROM covid_curated.VW_csse_covid_19_daily_reports_iso_names
WHERE country_region = 'Germany' 
--order by last_update

-- COMMAND ----------


