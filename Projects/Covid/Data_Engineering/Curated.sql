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



-- COMMAND ----------

SELECT date(last_update), Country_Region, sum(CONFIRMED) AS Total_Confirmed, sum(Deaths) AS Total_Deaths, sum(Recovered) AS Total_Recovered, sum(Active) AS Total_Active FROM covid_qualified.TBL_csse_covid_19_daily_reports 
WHERE Country_Region = 'Germany'
group by date(last_update), Country_Region
order by date(last_update), Country_Region

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
