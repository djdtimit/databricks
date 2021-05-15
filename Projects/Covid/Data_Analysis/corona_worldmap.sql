-- Databricks notebook source
SELECT Country_Region, Upper(alpha3) as alpha3, sum(Confirmed) / max(PopTotal) * 100 AS ratio_confirmed_pop, sum(Confirmed), max(Poptotal) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
having ratio_confirmed_pop is null
order by ratio_confirmed_pop DESC

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Confirmed) / max(PopTotal) * 100 AS ratio_confirmed_pop FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------


