-- Databricks notebook source
set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

-- COMMAND ----------

CREATE DATABASE if not exists COVID_CURATED

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **csse_covid_19_daily_reports**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Field description
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
-- MAGIC 
-- MAGIC All cases, deaths, and recoveries reported are based on the date of initial report. Exceptions to this are noted in the "Data Modification" and "Retrospective reporting of (probable) cases and deaths" subsections below.

-- COMMAND ----------

CREATE OR REPLACE VIEW COVID_CURATED.VW_CSSE

-- COMMAND ----------

With Group_by_date AS (
  SELECT
    Country_Region,
    date(last_update) as date,
    sum(Active) AS Active_Cases,
    sum(Recovered) AS Recoveries,
    sum(Deaths) AS Deaths,
    sum(Confirmed) AS Confirmed
  FROM
    covid_qualified.TBL_csse_covid_19_daily_reports
  WHERE
    COUNTRY_REGION = 'Germany'
  group by
    Country_Region,
    date(last_update)
  order by
    Country_Region,
    date(last_update)
),
calculate_lag as (
  SELECT
    Country_Region,
    date,
    Active_Cases,
    Recoveries,
    Deaths,
    Confirmed,
    lag(Active_Cases, 1) OVER (
      PARTITION BY Country_Region
      ORDER BY
        date
    ) AS LAG,
    lag(Confirmed, 1) OVER (
      PARTITION BY Country_Region
      ORDER BY
        date
    ) AS Confirmed_lag
  FROM
    GRoup_by_date
),
calculate_percentage_growth as (
  SELECT
    Country_Region,
    date,
    Active_Cases,
    Recoveries,
    Deaths,
    Confirmed,
    LAG,
    (Active_Cases - LAG) / LAG * 100 AS Percentage_Growth_Active_Cases,
    Confirmed_lag
  FROM
    calculate_lag
)
SELECT
  Country_Region,
  date,
  Active_Cases,
  Recoveries,
  Deaths,
  Confirmed,
  LAG,
  Percentage_Growth_Active_Cases,
  avg(Percentage_Growth_Active_Cases) OVER (
    Order By
      Date Rows BETWEEN 7 Preceding
      AND Current Row
  ) as smoothed_Percentage_Growth_Active_Cases,
  Confirmed - Confirmed_lag AS Confirmed_growth,
  avg(Confirmed - Confirmed_lag) OVER (
    Order By
      Date Rows BETWEEN 7 Preceding
      AND Current Row
  ) as smoothed_Confirmed_growth
FROM
  calculate_percentage_growth

-- COMMAND ----------

With Group_by_date AS (
  SELECT
    Country_Region,
    date(last_update) as date,
    sum(Active) AS Active_Cases,
    sum(Recovered) AS Recoveries,
    sum(Deaths) AS Deaths,
    sum(Confirmed) AS Confirmed
  FROM
    covid_qualified.TBL_csse_covid_19_daily_reports
  WHERE
    COUNTRY_REGION = 'Germany'
  group by
    Country_Region,
    date(last_update)
  order by
    Country_Region,
    date(last_update)
),
calculate_lag as (
  SELECT
    Country_Region,
    date,
    Active_Cases,
    Recoveries,
    Deaths,
    Confirmed,
    lag(Active_Cases, 1) OVER (
      PARTITION BY Country_Region
      ORDER BY
        date
    ) AS LAG,
    lag(Confirmed, 1) OVER (
      PARTITION BY Country_Region
      ORDER BY
        date
    ) AS Confirmed_growth
  FROM
    GRoup_by_date
),
calculate_percentage_growth as (
  SELECT
    Country_Region,
    date,
    Active_Cases,
    Recoveries,
    Deaths,
    Confirmed,
    LAG,
    (Active_Cases - LAG) / LAG * 100 AS Percentage_Growth_Active_Cases,
    Confirmed_growth
  FROM
    calculate_lag
)
SELECT
  Country_Region,
  date,
  Active_Cases,
  Recoveries,
  Deaths,
  Confirmed,
  LAG,
  Percentage_Growth_Active_Cases,
  IFNULL(log(Percentage_Growth_Active_Cases),0),
  avg(Percentage_Growth_Active_Cases) OVER (
    Order By
      Date Rows BETWEEN 7 Preceding
      AND Current Row
  ) as smoothed_Percentage_Growth_Active_Cases,
  Confirmed_growth
FROM
  calculate_percentage_growth

-- COMMAND ----------

With Group_by_date AS (
  SELECT
    Country_Region,
    date(last_update) as date,
    sum(Active) AS Active_Cases,
    sum(Recovered) AS Recoveries,
    sum(Deaths) AS Deaths,
    sum(Confirmed) AS Confirmed
  FROM
    covid_qualified.TBL_csse_covid_19_daily_reports
--   WHERE
--     COUNTRY_REGION = 'Germany'
  group by
    Country_Region,
    date(last_update)
  order by
    Country_Region,
    date(last_update)
),
calculate_lag as (
  SELECT
    Country_Region,
    date,
    Active_Cases,
    Recoveries,
    Deaths,
    Confirmed,
    lag(Active_Cases, 1) OVER (
      PARTITION BY Country_Region
      ORDER BY
        date
    ) AS LAG
  FROM
    GRoup_by_date
)
SELECT
  Country_Region,
  date,
  Active_Cases,
  Recoveries,
  Deaths,
  Confirmed,
  (Active_Cases - LAG) / LAG * 100 AS Percentage_Growth_Active_Cases
FROM
  calculate_lag

-- COMMAND ----------

SELECT date(last_update), Country_Region, sum(CONFIRMED) AS Total_Confirmed, sum(Deaths) AS Total_Deaths, sum(Recovered) AS Total_Recovered, sum(Active) AS Total_Active FROM covid_qualified.TBL_csse_covid_19_daily_reports 
WHERE Country_Region = 'Germany'
group by date(last_update), Country_Region
order by date(last_update), Country_Region

-- COMMAND ----------

SELECT distinct country_region FROM covid_qualified.TBL_csse_covid_19_daily_reports cdr
LEFT JOIN covid_qualified.TBL_country_iso_data cid
On cdr.Country_region = cid.name
where name is null

-- COMMAND ----------

SELECT * FROM covid_qualified.TBL_country_iso_data

-- COMMAND ----------

SELECT
  last_update,
  country_region,
  sum(Daily_Confirmed)
FROM (
SELECT
  *,
  Confirmed - lag AS Daily_Confirmed
FROM
  (
    SELECT
      date(last_update),
      Country_Region,
      Confirmed,
      LAG(CONFIRMED) OVER (
        PARTITION BY date(last_update),
        Country_Region
        ORDER BY
          CONFIRMED
      ) AS lag
    FROM
      covid_qualified.TBL_csse_covid_19_daily_reports
  )
)
WHERE
  Country_Region = 'Germany'
group by
  last_update,
  country_region
order by
  last_update,
  Country_Region

-- COMMAND ----------

SELECT date(last_update), Confirmed, Active FROM covid_qualified.TBL_csse_covid_19_daily_reports
WHERE
  Country_Region = 'Germany'
  order by date(last_update)

-- COMMAND ----------

SELECT date(Meldedatum), sum(AnzahlFall)  FROM covid_qualified.TBL_RKI_COVID19
group by date(Meldedatum)
order by date(Meldedatum)

-- COMMAND ----------

SELECT * FROM covid_qualified.TBL_RKI_COVID19

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Covid_Cases_Vaccinations**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Beschreibung der Daten des RKI Covid-19-Dashboards (https://corona.rki.de)
-- MAGIC 
-- MAGIC Dem Dashboard liegen aggregierte Daten der gemäß IfSG von den Gesundheitsämtern an das RKI übermittelten Covid-19-Fälle zu Grunde
-- MAGIC Mit den Daten wird der tagesaktuelle Stand (00:00 Uhr) abgebildet und es werden die Veränderungen bei den Fällen und Todesfällen zum Vortag dargstellt
-- MAGIC In der Datenquelle sind folgende Parameter enthalten:
-- MAGIC 
-- MAGIC - IdBundesland: Id des Bundeslands des Falles mit 1=Schleswig-Holstein bis 16=Thüringen
-- MAGIC - Bundesland: Name des Bundeslanes
-- MAGIC - Landkreis ID: Id des Landkreises des Falles in der üblichen Kodierung 1001 bis 16077=LK Altenburger Land
-- MAGIC - Landkreis: Name des Landkreises
-- MAGIC - Altersgruppe: Altersgruppe des Falles aus den 6 Gruppe 0-4, 5-14, 15-34, 35-59, 60-79, 80+ sowie unbekannt
-- MAGIC - Altersgruppe2: Altersgruppe des Falles aus 5-Jahresgruppen 0-4, 5-9, 10-14, ..., 75-79, 80+ sowie unbekannt
-- MAGIC - Geschlecht: Geschlecht des Falles M0männlich, W=weiblich und unbekannt
-- MAGIC - AnzahlFall: Anzahl der Fälle in der entsprechenden Gruppe
-- MAGIC - AnzahlTodesfall: Anzahl der Todesfälle in der entsprechenden Gruppe
-- MAGIC - Meldedatum: Datum, wann der Fall dem Gesundheitsamt bekannt geworden ist
-- MAGIC - Datenstand: Datum, wann der Datensatz zuletzt aktualisiert worden ist
-- MAGIC - NeuerFall: 
-- MAGIC 0: Fall ist in der Publikation für den aktuellen Tag und in der für den Vortag enthalten
-- MAGIC 1: Fall ist nur in der aktuellen Publikation enthalten
-- MAGIC -1: Fall ist nur in der Publikation des Vortags enthalten
-- MAGIC damit ergibt sich: Anzahl Fälle der aktuellen Publikation als Summe(AnzahlFall), wenn NeuerFall in (0,1); Delta zum Vortag als Summe(AnzahlFall) wenn NeuerFall in (-1,1)
-- MAGIC - NeuerTodesfall:
-- MAGIC 0: Fall ist in der Publikation für den aktuellen Tag und in der für den Vortag jeweils ein Todesfall
-- MAGIC 1: Fall ist in der aktuellen Publikation ein Todesfall, nicht jedoch in der Publikation des Vortages
-- MAGIC -1: Fall ist in der aktuellen Publikation kein Todesfall, jedoch war er in der Publikation des Vortags ein Todesfall
-- MAGIC -9: Fall ist weder in der aktuellen Publikation noch in der des Vortages ein Todesfall
-- MAGIC damit ergibt sich: Anzahl Todesfälle der aktuellen Publikation als Summe(AnzahlTodesfall) wenn NeuerTodesfall in (0,1); Delta zum Vortag als Summe(AnzahlTodesfall) wenn NeuerTodesfall in (-1,1)
-- MAGIC - Referenzdatum: Erkrankungsdatum bzw. wenn das nicht bekannt ist, das Meldedatum
-- MAGIC - AnzahlGenesen: Anzahl der Genesenen in der entsprechenden Gruppe
-- MAGIC - NeuGenesen:
-- MAGIC 0: Fall ist in der Publikation für den aktuellen Tag und in der für den Vortag jeweils Genesen
-- MAGIC 1: Fall ist in der aktuellen Publikation Genesen, nicht jedoch in der Publikation des Vortages
-- MAGIC -1: Fall ist in der aktuellen Publikation nicht Genesen, jedoch war er in der Publikation des Vortags Genesen
-- MAGIC -9: Fall ist weder in der aktuellen Publikation noch in der des Vortages Genesen 
-- MAGIC damit ergibt sich: Anzahl Genesen der aktuellen Publikation als Summe(AnzahlGenesen) wenn NeuGenesen in (0,1); Delta zum Vortag als Summe(AnzahlGenesen) wenn NeuGenesen in (-1,1)
-- MAGIC - IstErkrankungsbeginn: 1, wenn das Refdatum der Erkrankungsbeginn ist, 0 sonst

-- COMMAND ----------

SELECT * FROM covid_qualified.tbl_germany_deliveries_timeseries_v2
WHERE impfstoff = 'comirnaty'
order by date, impfstoff

-- COMMAND ----------

SELECT * FROM covid_qualified.tbl_germany_deliveries_timeseries_v2
WHERE region = 'DE-SH'
order by date, impfstoff

-- COMMAND ----------

SELECT * FROM covid_qualified.tbl_germany_deliveries_timeseries_v2

-- COMMAND ----------

SELECT * FROM covid_qualified.tbl_germany_vaccinations_by_state_v1

-- COMMAND ----------

SELECT * FROM covid_qualified.tbl_germany_vaccinations_timeseries_v2
order by date

-- COMMAND ----------

SELECT date, impf_quote_erst * 100 AS impf_quote_erst_in_Percent, impf_quote_voll * 100 AS impf_quote_voll_in_Percent FROM covid_qualified.tbl_germany_vaccinations_timeseries_v2
order by date

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_curated.TBL_csse_covid_19_daily_reports_iso_names
USING DELTA
LOCATION '/mnt/covid/Curated/TBL_csse_covid_19_daily_reports_iso_names/'
AS 
SELECT
  *
FROM
  covid_curated.VW_csse_covid_19_daily_reports_iso_names

-- COMMAND ----------

CREATE OR REPLACE VIEW covid_curated.VW_csse_covid_19_daily_reports_iso_names AS
SELECT
  ccd.FIPS,
  ccd.Admin2,
  ccd.Province_State,
  ccd.Country_Region,
  ccd.last_update,
  ccd.Latitude,
  ccd.Longitude,
  ccd.Confirmed,
  ccd.Deaths,
  ccd.Recovered,
  ccd.Active,
  ccd.Incidence_Rate,
  ccd.Case_Fatality_Ratio,
  cid.alpha3,
  wpp.PopTotal,
  ccd._INSERT_TS
FROM
  covid_qualified.TBL_csse_covid_19_daily_reports ccd
  LEFT JOIN covid_qualified.tbl_country_iso_data cid on ccd.Country_Region = cid.name
  left join covid_qualified.TBL_WPP2019_TotalPopulationBySex wpp
on ccd.country_region = wpp.location and year(ccd.last_update) = wpp.time
