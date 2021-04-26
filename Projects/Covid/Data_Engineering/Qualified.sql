-- Databricks notebook source
CREATE DATABASE covid_qualified

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **csse_covid_19_daily_reports**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_csse_covid_19_daily_reports AS
SELECT
  NULLIF(FIPS, '') :: INT AS FIPS,
  NULLIF(NULLIF(Admin2, ''), 'None') AS Admin2,
  NULLIF(NULLIF(Province_State, ''), 'None') AS Province_State,
  NULLIF(Country_Region, '') AS Country_Region,
  CASE
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 15 THEN NULLIF(to_timestamp(last_update, 'M/dd/yyyy HH:mm'), '')
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 14
    AND SUBSTRING(last_update, 9, 1) = ' ' THEN NULLIF(to_timestamp(last_update, 'M/d/yyyy HH:mm'), '')
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 14
    AND SUBSTRING(last_update, 10, 1) = ' ' THEN NULLIF(to_timestamp(last_update, 'M/dd/yyyy H:mm'), '')
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 13
    AND SUBSTRING(last_update, 8, 1) = ' ' THEN NULLIF(to_timestamp(last_update, 'M/dd/yy HH:mm'), '')
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 13
    AND SUBSTRING(last_update, 9, 1) = ' ' THEN NULLIF(to_timestamp(last_update, 'M/d/yyyy H:mm'), '')
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 12
    AND SUBSTRING(last_update, 7, 1) = ' ' THEN NULLIF(to_timestamp(last_update, 'M/d/yy HH:mm'), '')
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 12
    AND SUBSTRING(last_update, 8, 1) = ' ' THEN NULLIF(to_timestamp(last_update, 'M/dd/yy H:mm'), '')
    WHEN last_update like '%/%'
    AND LENGTH(last_update) = 11 THEN NULLIF(to_timestamp(last_update, 'M/d/yy H:mm'), '')
    ELSE NULLIF(to_timestamp(last_update), '')
  END AS Last_Update,
  NULLIF(Latitude, '') :: DECIMAL(38, 15) AS Latitude,
  NULLIF(Longitude, '') :: DECIMAL(38, 15) AS Longitude,
  NULLIF(Confirmed, '') :: INT AS Confirmed,
  NULLIF(Deaths, '') :: INT AS Deaths,
  NULLIF(Recovered, '') :: INT AS Recovered,
  NULLIF(Active, '') :: INT AS Active,
  NULLIF(Combined_Key, '') AS Combined_Key,
  NULLIF(Incidence_Rate, '') :: DECIMAL(38, 15) AS Incidence_Rate,
  NULLIF(Case_Fatality_Ratio, '') :: DECIMAL(38, 15) AS Case_Fatality_Ratio,
  _source
FROM
  COVID_RAW.TBL_csse_covid_19_daily_reports

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_csse_covid_19_daily_reports USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_csse_covid_19_daily_reports/' AS
SELECT
  *
FROM
  covid_qualified.VW_csse_covid_19_daily_reports

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_vaccinations_timeseries_v2**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.vw_germany_vaccinations_timeseries_v2 AS
select
  NULLIF(date, '') :: DATE AS date,
  NULLIF(dosen_kumulativ, '') :: INT AS dosen_kumulativ,
  NULLIF(dosen_differenz_zum_vortag, '') :: INT AS dosen_differenz_zum_vortag,
  NULLIF(dosen_erst_differenz_zum_vortag, '') :: INT AS dosen_erst_differenz_zum_vortag,
  NULLIF(dosen_zweit_differenz_zum_vortag, '') :: INT AS dosen_zweit_differenz_zum_vortag,
  NULLIF(dosen_biontech_kumulativ, '') :: INT AS dosen_biontech_kumulativ,
  NULLIF(dosen_moderna_kumulativ, '') :: INT AS dosen_moderna_kumulativ,
  NULLIF(dosen_astrazeneca_kumulativ, '') :: INT AS dosen_astrazeneca_kumulativ,
  NULLIF(personen_erst_kumulativ, '') :: INT AS personen_erst_kumulativ,
  NULLIF(personen_voll_kumulativ, '') :: INT AS personen_voll_kumulativ,
  NULLIF(impf_quote_erst, '') :: DECIMAL(10, 3) AS impf_quote_erst,
  NULLIF(impf_quote_voll, '') :: DECIMAL(10, 3) AS impf_quote_voll,
  NULLIF(indikation_alter_dosen, '') :: INT AS indikation_alter_dosen,
  NULLIF(indikation_beruf_dosen, '') :: INT AS indikation_beruf_dosen,
  NULLIF(indikation_medizinisch_dosen, '') :: INT AS indikation_medizinisch_dosen,
  NULLIF(indikation_pflegeheim_dosen, '') :: INT AS indikation_pflegeheim_dosen,
  NULLIF(indikation_alter_erst, '') :: INT AS indikation_alter_erst,
  NULLIF(indikation_beruf_erst, '') :: INT AS indikation_beruf_erst,
  NULLIF(indikation_medizinisch_erst, '') :: INT AS indikation_medizinisch_erst,
  NULLIF(indikation_pflegeheim_erst, '') :: INT AS indikation_pflegeheim_erst,
  NULLIF(indikation_alter_voll, '') :: INT AS indikation_alter_voll,
  NULLIF(indikation_beruf_voll, '') :: INT AS indikation_beruf_voll,
  NULLIF(indikation_medizinisch_voll, '') :: INT AS indikation_medizinisch_voll,
  NULLIF(indikation_pflegeheim_voll, '') :: INT AS indikation_pflegeheim_voll
FROM
  COVID_RAW.TBL_germany_vaccinations_timeseries_v2

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_germany_vaccinations_timeseries_v2 USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_germany_vaccinations_timeseries_v2/' AS
SELECT
  *
FROM
  covid_qualified.vw_germany_vaccinations_timeseries_v2

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_deliveries_timeseries_v2**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_germany_deliveries_timeseries_v2 AS
select
  NULLIF(date, '') :: DATE AS date,
  NULLIF(impfstoff, '') AS impfstoff,
  NULLIF(region, '') AS region,
  NULLIF(dosen, '') :: INT AS dosen
from
  COVID_RAW.TBL_germany_deliveries_timeseries_v2

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_germany_deliveries_timeseries_v2 USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_germany_deliveries_timeseries_v2/' AS
SELECT
  *
FROM
  covid_qualified.vw_germany_deliveries_timeseries_v2

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **germany_vaccinations_by_state_v1**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_germany_vaccinations_by_state_v1 AS
SELECT
  NULLIF(code, '') AS code,
  NULLIF(vaccinationsTotal, '') :: INT AS vaccinationsTotal,
  NULLIF(peopleFirstTotal, '') :: INT AS peopleFirstTotal,
  NULLIF(peopleFullTotal, '') :: INT AS peopleFullTotal
FROM
  COVID_RAW.TBL_germany_vaccinations_by_state_v1

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_germany_vaccinations_by_state_v1 USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_germany_vaccinations_by_state_v1/' AS
SELECT
  *
FROM
  covid_qualified.vw_germany_vaccinations_by_state_v1

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Altersgruppen**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_RKI_Altersgruppen AS
SELECT
  NULLIF(properties.AdmUnitId, '') AS AdmUnitId,
  NULLIF(properties.Altersgruppe, '') AS Altersgruppe,
  NULLIF(properties.AnzFall100kM, '') :: DECIMAL(10, 2) AS AnzFall100kM,
  NULLIF(properties.AnzFall100kW, '') :: DECIMAL(10, 2) AS AnzFall100kW,
  NULLIF(properties.AnzFallM, '') :: INT AS AnzFallM,
  NULLIF(properties.AnzFallW, '') :: INT AS AnzFallW,
  NULLIF(properties.AnzTodesfall100kM, '') :: DECIMAL(10, 2) AS AnzTodesfall100kM,
  NULLIF(properties.AnzTodesfall100kW, '') :: DECIMAL(10, 2) AS AnzTodesfall100kW,
  NULLIF(properties.AnzTodesfallM, '') :: INT AS AnzTodesfallM,
  NULLIF(properties.AnzTodesfallW, '') :: INT AS AnzTodesfallW,
  NULLIF(properties.BundeslandId, '') AS BundeslandId,
  NULLIF(properties.ObjectId, '') AS ObjectId
FROM
  COVID_RAW.TBL_RKI_Altersgruppen
WHERE
  NULLIF(properties.ObjectId, '') is not null

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_RKI_Altersgruppen USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_RKI_Altersgruppen/' AS
SELECT
  *
FROM
  covid_qualified.vw_RKI_Altersgruppen

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_COVID19**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_RKI_COVID19 AS
SELECT
  NULLIF(properties.Altersgruppe, '') AS Altersgruppe,
  NULLIF(properties.Altersgruppe2, '') AS Altersgruppe2,
  NULLIF(properties.AnzahlFall, '') :: INT AS AnzahlFall,
  NULLIF(properties.AnzahlGenesen, '') :: INT AS AnzahlGenesen,
  NULLIF(properties.AnzahlTodesfall, '') :: INT AS AnzahlTodesfall,
  NULLIF(properties.Bundesland, '') AS Bundesland,
  NULLIF(
    to_timestamp(
      trim(replace(properties.Datenstand, ' Uhr', '')),
      'dd.MM.yyyy, HH:mm'
    ),
    ''
  ) AS Datenstand,
  NULLIF(properties.Geschlecht, '') AS Geschlecht,
  NULLIF(properties.IdBundesland, '') AS IdBundesland,
  NULLIF(properties.IdLandkreis, '') AS IdLandkreis,
  NULLIF(properties.IstErkrankungsbeginn, '') :: INT AS IstErkrankungsbeginn,
  NULLIF(properties.Landkreis, '') AS Landkreis,
  NULLIF(properties.Meldedatum, '') :: TIMESTAMP AS Meldedatum,
  NULLIF(properties.NeuGenesen, '') :: INT AS NeuGenesen,
  NULLIF(properties.NeuerFall, '') :: INT AS NeuerFall,
  NULLIF(properties.NeuerTodesfall, '') :: INT AS NeuerTodesfall,
  NULLIF(properties.ObjectId, '') AS ObjectId,
  NULLIF(properties.Refdatum, '') :: TIMESTAMP AS Refdatum
FROM
  COVID_RAW.TBL_RKI_COVID19
WHERE
  NULLIF(properties.ObjectId, '') is not null

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_RKI_COVID19 USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_RKI_COVID19/' AS
SELECT
  *
FROM
  covid_qualified.vw_RKI_COVID19

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Corona_Landkreise**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_RKI_Corona_Landkreise AS
SELECT
  geometry.coordinates AS GEOMETRY,
  NULLIF(properties.ADE, '') :: INT AS ADE,
  NULLIF(properties.AGS, '') AS AGS,
  NULLIF(properties.AGS_0, '') AS AGS_0,
  NULLIF(properties.AdmUnitId, '') AS AdmUnitId,
  NULLIF(properties.BEM, '') AS BEM,
  NULLIF(properties.BEZ, '') AS BEZ,
  NULLIF(properties.BL, '') AS BL,
  NULLIF(properties.BL_ID, '') :: INT AS BL_ID,
  NULLIF(properties.BSG, '') :: INT AS BSG,
  NULLIF(properties.DEBKG_ID, '') AS DEBKG_ID,
  NULLIF(properties.EWZ, '') :: INT AS EWZ,
  NULLIF(properties.EWZ_BL, '') :: INT AS EWZ_BL,
  NULLIF(properties.FK_S3, '') AS FK_S3,
  NULLIF(properties.GEN, '') AS GEN,
  NULLIF(properties.GF, '') :: INT AS GF,
  NULLIF(properties.IBZ, '') :: INT AS IBZ,
  NULLIF(properties.KFL, '') :: DECIMAL(10, 3) AS KFL,
  NULLIF(properties.NBD, '') AS NBD,
  NULLIF(properties.NUTS, '') AS NUTS,
  NULLIF(properties.OBJECTID, '') :: INT AS OBJECTID,
  NULLIF(properties.RS, '') AS RS,
  NULLIF(properties.RS_0, '') AS RS_0,
  NULLIF(properties.SDV_RS, '') AS SDV_RS,
  NULLIF(properties.SHAPE_Area, '') :: DECIMAL(20, 18) AS SHAPE_Area,
  NULLIF(properties.SHAPE_Length, '') :: DECIMAL(20, 18) AS SHAPE_Length,
  NULLIF(properties.SN_G, '') AS SN_G,
  NULLIF(properties.SN_K, '') AS SN_K,
  NULLIF(properties.SN_L, '') AS SN_L,
  NULLIF(properties.SN_R, '') AS SN_R,
  NULLIF(properties.SN_V1, '') AS SN_V1,
  NULLIF(properties.SN_V2, '') AS SN_V2,
  NULLIF(
    to_timestamp(properties.WSK, 'yyyy/MM/dd HH:mm:ss.SSS'),
    ''
  ) AS WSK,
  NULLIF(properties.cases, '') :: INT AS cases,
  NULLIF(properties.cases7_bl, '') :: INT AS cases7_bl,
  NULLIF(properties.cases7_bl_per_100k, '') :: DECIMAL(38, 15) AS cases7_bl_per_100k,
  NULLIF(properties.cases7_lk, '') :: INT AS cases7_lk,
  NULLIF(properties.cases7_per_100k, '') :: DECIMAL(38, 15) AS cases7_per_100k,
  NULLIF(properties.cases7_per_100k_txt, '') :: DECIMAL(38, 15) AS cases7_per_100k_txt,
  NULLIF(properties.cases_per_100k, '') :: DECIMAL(38, 15) AS cases_per_100k,
  NULLIF(properties.cases_per_population, '') :: DECIMAL(38, 15) AS cases_per_population,
  NULLIF(properties.county, '') AS county,
  NULLIF(properties.death7_bl, '') :: INT AS death7_bl,
  NULLIF(properties.death7_lk, '') :: INT AS death7_lk,
  NULLIF(properties.death_rate, '') :: DECIMAL(38, 15) AS death_rate,
  NULLIF(properties.deaths, '') :: INT AS deaths,
  NULLIF(
    to_timestamp(
      trim(replace(properties.last_update, ' Uhr', '')),
      'dd.MM.yyyy, HH:mm'
    ),
    ''
  ) AS last_update,
  NULLIF(properties.recovered, '') AS recovered
from
  COVID_RAW.TBL_RKI_Corona_Landkreise
WHERE
  geometry.coordinates is not null

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_RKI_Corona_Landkreise USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_RKI_Corona_Landkreise/' AS
SELECT
  *
FROM
  covid_qualified.vw_RKI_Corona_Landkreise

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_Corona_Bundeslaender**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_RKI_Corona_Bundeslaender AS
SELECT
  geometry.coordinates AS GEOMETRY,
  NULLIF(properties.AGS_TXT, '') AS AGS_TXT,
  NULLIF(properties.AdmUnitId, '') AS AdmUnitId,
  NULLIF(
    to_timestamp(
      replace(replace(properties.Aktualisierung, 'T', ' '), 'Z', ''),
      'yyyy-MM-dd HH:mm:ss'
    ),
    ''
  ) AS Aktualisierung,
  NULLIF(properties.Death, '') :: INT AS Death,
  NULLIF(properties.Fallzahl, '') :: INT AS Fallzahl,
  NULLIF(properties.GlobalID, '') AS GlobalID,
  NULLIF(properties.LAN_ew_AGS, '') AS LAN_ew_AGS,
  NULLIF(properties.LAN_ew_BEZ, '') AS LAN_ew_BEZ,
  NULLIF(properties.LAN_ew_EWZ, '') :: INT AS LAN_ew_EWZ,
  NULLIF(properties.LAN_ew_GEN, '') AS LAN_ew_GEN,
  NULLIF(properties.OBJECTID, '') AS OBJECTID,
  NULLIF(properties.OBJECTID_1, '') AS OBJECTID_1,
  NULLIF(properties.SHAPE_Area, '') :: DECIMAL(38, 20) AS SHAPE_Area,
  NULLIF(properties.SHAPE_Length, '') :: DECIMAL(38, 20) AS SHAPE_Length,
  NULLIF(properties.cases7_bl, '') :: INT AS cases7_bl,
  NULLIF(properties.cases7_bl_per_100k, '') :: DECIMAL(38, 20) AS cases7_bl_per_100k,
  NULLIF(properties.cases7_bl_per_100k_txt, '') :: DECIMAL(5, 2) AS cases7_bl_per_100k_txt,
  NULLIF(properties.death7_bl, '') :: INT AS death7_bl,
  NULLIF(properties.faelle_100000_EW, '') :: DECIMAL(38, 20) AS faelle_100000_EW
FROM
  COVID_RAW.TBL_RKI_Corona_Bundeslaender
WHERE
  geometry.coordinates is not null

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_RKI_Corona_Bundeslaender USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_RKI_Corona_Bundeslaender/' AS
SELECT
  *
FROM
  covid_qualified.vw_RKI_Corona_Bundeslaender

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RKI_key_data**

-- COMMAND ----------

CREATE
OR REPLACE VIEW covid_qualified.VW_RKI_key_data AS
SELECT
  NULLIF(properties.AdmUnitId, '') AS AdmUnitId,
  NULLIF(properties.AnzAktiv, '') :: INT AS AnzAktiv,
  NULLIF(properties.AnzAktivNeu, '') :: INT AS AnzAktivNeu,
  NULLIF(properties.AnzFall, '') :: INT AS AnzFall,
  NULLIF(properties.AnzFall7T, '') :: INT AS AnzFall7T,
  NULLIF(properties.AnzFallNeu, '') :: INT AS AnzFallNeu,
  NULLIF(properties.AnzGenesen, '') :: INT AS AnzGenesen,
  NULLIF(properties.AnzGenesenNeu, '') :: INT AS AnzGenesenNeu,
  NULLIF(properties.AnzTodesfall, '') :: INT AS AnzTodesfall,
  NULLIF(properties.AnzTodesfallNeu, '') :: INT AS AnzTodesfallNeu,
  NULLIF(properties.BundeslandId, '') AS BundeslandId,
  NULLIF(properties.Inz7T, '') :: DECIMAL(10, 3) AS Inz7T,
  NULLIF(properties.ObjectId, '') AS ObjectId
FROM
  COVID_RAW.TBL_RKI_key_data
WHERE
  properties is not null

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS covid_qualified.TBL_RKI_key_data USING DELTA LOCATION '/mnt/kaggle/Covid/Qualified/TBL_RKI_key_data/' AS
SELECT
  *
FROM
  covid_qualified.vw_RKI_key_data

-- COMMAND ----------


