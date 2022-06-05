-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Confirmed**

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Confirmed) AS Confirmed FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Confirmed) / max(PopTotal) * 100 AS ratio_confirmed_pop FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Deaths**

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Deaths) AS Deaths FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Deaths) / max(PopTotal) * 100 AS ratio_deaths_pop FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **active cases**

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Active) AS Active FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Active) / max(PopTotal) * 100 AS ratio_active_pop FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **recovered**

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Recovered) AS Recovered FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, sum(Recovered) / max(PopTotal) * 100 AS ratio_recovered_pop FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **incidence rate**

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, mean(incidence_rate) AS incidence_rate FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **case fatality ration**

-- COMMAND ----------

SELECT Country_Region, Upper(alpha3) as alpha3, mean(case_fatality_ratio) AS case_fatality_ratio FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names
WHERE date(last_update) = (SELECT max(date(last_update)) FROM covid_curated.TBL_csse_covid_19_daily_reports_iso_names)
group by Country_Region, Upper(alpha3)
order by country_region

-- COMMAND ----------


