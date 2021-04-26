# Databricks notebook source
import databricks.koalas as ks
from pyspark.sql.functions import input_file_name

# COMMAND ----------

def write_csv_into_raw(ingestion_load_path, raw_save_path):
  df = ks.read_csv(ingestion_load_path,dtype=str)
  df['_source'] = input_file_name()
  df.to_delta(raw_save_path,mode='overwrite', index=False)

# COMMAND ----------

def write_json_into_raw(ingestion_load_path, raw_save_path):
  df = ks.read_json(ingestion_load_path,dtype=str)
  df['_source'] = input_file_name()
  df.to_delta(raw_save_path,mode='overwrite', index=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS COVID_RAW

# COMMAND ----------

# MAGIC %md
# MAGIC **csse_covid_19_daily_reports**

# COMMAND ----------

def rename_columns(s) -> str:
  new_column = s.replace(' ','_').replace('/','_').replace('-','_')
  
  if 'Long' in s:
    new_column = 'Longitude'
  if 'Lat' in s:
    new_column = 'Latitude'
  if 'Incid' in s:
    new_column = 'Incidence_Rate'
  return new_column

mount_point = '/mnt/kaggle/Covid/Ingestion/csse_covid_19_daily_reports/'

file_list = [file.name for file in dbutils.fs.ls("dbfs:{}".format(mount_point))]

for file in file_list:
  loadFile = "{0}/{1}".format(mount_point, file)
  df_csse_covid_19_daily_reports = ks.read_csv(loadFile,dtype=str)
  
  df_csse_covid_19_daily_reports['_source'] = input_file_name()
  
  df_renamed = df_csse_covid_19_daily_reports.rename(rename_columns, axis='columns')
  
  if 'Active' not in df_renamed.columns:
    df_renamed['Active'] = ''
   
  if 'Latitude' not in df_renamed.columns:
    df_renamed['Latitude'] = ''

  if 'Longitude' not in df_renamed.columns:
    df_renamed['Longitude'] = ''

  if 'FIPS' not in df_renamed.columns:
    df_renamed['FIPS'] = ''

  if 'Admin2' not in df_renamed.columns:
    df_renamed['Admin2'] = ''

  if 'Combined_Key' not in df_renamed.columns:
    df_renamed['Combined_Key'] = ''

  if 'Incidence_Rate' not in df_renamed.columns:
    df_renamed['Incidence_Rate'] = ''

  if 'Case_Fatality_Ratio' not in df_renamed.columns:
    df_renamed['Case_Fatality_Ratio'] = ''
    
  df_final = df_renamed[["FIPS","Admin2","Province_State","Country_Region","Last_Update","Latitude","Longitude","Confirmed","Deaths","Recovered","Active","Combined_Key","Incidence_Rate","Case_Fatality_Ratio","_source"]]
  
  df_final.to_delta(path= '/mnt/kaggle/Covid/Raw/TBL_csse_covid_19_daily_reports/',mode='append', index=False)
  
  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_csse_covid_19_daily_reports
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_csse_covid_19_daily_reports/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_csse_covid_19_daily_reports

# COMMAND ----------

# MAGIC %md
# MAGIC **germany_vaccinations_timeseries_v2**

# COMMAND ----------

mnt_point_germany_vaccinations_timeseries_v2_Ingestion = '/mnt/kaggle/Covid/Ingestion/germany_vaccinations_timeseries_v2/'
mnt_point_germany_vaccinations_timeseries_v2_Raw = '/mnt/kaggle/Covid/Raw/TBL_germany_vaccinations_timeseries_v2/'

write_csv_into_raw(mnt_point_germany_vaccinations_timeseries_v2_Ingestion, mnt_point_germany_vaccinations_timeseries_v2_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_germany_vaccinations_timeseries_v2
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_germany_vaccinations_timeseries_v2/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_germany_vaccinations_timeseries_v2

# COMMAND ----------

# MAGIC %md
# MAGIC **germany_deliveries_timeseries_v2**

# COMMAND ----------

mnt_point_germany_deliveries_timeseries_v2_Ingestion = '/mnt/kaggle/Covid/Ingestion/germany_deliveries_timeseries_v2/'
mnt_point_germany_deliveries_timeseries_v2_Raw = '/mnt/kaggle/Covid/Raw/TBL_germany_deliveries_timeseries_v2/'

write_csv_into_raw(mnt_point_germany_deliveries_timeseries_v2_Ingestion, mnt_point_germany_deliveries_timeseries_v2_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_germany_deliveries_timeseries_v2
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_germany_deliveries_timeseries_v2/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_germany_deliveries_timeseries_v2

# COMMAND ----------

# MAGIC %md
# MAGIC **germany_vaccinations_by_state_v1**

# COMMAND ----------

mnt_point_germany_vaccinations_by_state_v1_Ingestion = '/mnt/kaggle/Covid/Ingestion/germany_vaccinations_by_state_v1/'
mnt_point_germany_vaccinations_by_state_v1_Raw = '/mnt/kaggle/Covid/Raw/TBL_germany_vaccinations_by_state_v1/'

write_csv_into_raw(mnt_point_germany_vaccinations_by_state_v1_Ingestion, mnt_point_germany_vaccinations_by_state_v1_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_germany_vaccinations_by_state_v1
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_germany_vaccinations_by_state_v1/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_germany_vaccinations_by_state_v1

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Altersgruppen**

# COMMAND ----------

mnt_point_RKI_Altersgruppen_Ingestion = '/mnt/kaggle/Covid/Ingestion/RKI_Altersgruppen/'
mnt_point_RKI_Altersgruppen_Raw = '/mnt/kaggle/Covid/Raw/TBL_RKI_Altersgruppen/'

write_json_into_raw(mnt_point_RKI_Altersgruppen_Ingestion, mnt_point_RKI_Altersgruppen_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Altersgruppen
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_RKI_Altersgruppen/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_RKI_Altersgruppen

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_COVID19**

# COMMAND ----------

mnt_point_RKI_COVID19_Ingestion = '/mnt/kaggle/Covid/Ingestion/RKI_COVID19/'
mnt_point_RKI_COVID19_Raw = '/mnt/kaggle/Covid/Raw/TBL_RKI_COVID19/'

write_json_into_raw(mnt_point_RKI_COVID19_Ingestion, mnt_point_RKI_COVID19_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_COVID19
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_RKI_COVID19/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_RKI_COVID19

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Corona_Landkreise**

# COMMAND ----------

mnt_point_RKI_Corona_Landkreise_Ingestion = '/mnt/kaggle/Covid/Ingestion/RKI_Corona_Landkreise/'
mnt_point_RKI_Corona_Landkreise_Raw = '/mnt/kaggle/Covid/Raw/TBL_RKI_Corona_Landkreise/'

write_json_into_raw(mnt_point_RKI_Corona_Landkreise_Ingestion, mnt_point_RKI_Corona_Landkreise_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Corona_Landkreise
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_RKI_Corona_Landkreise/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_RKI_Corona_Landkreise

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Corona_Bundeslaender**

# COMMAND ----------

mnt_point_RKI_Corona_Bundeslaender_Ingestion = '/mnt/kaggle/Covid/Ingestion/RKI_Corona_Bundeslaender/'
mnt_point_RKI_Corona_Bundeslaender_Raw = '/mnt/kaggle/Covid/Raw/TBL_RKI_Corona_Bundeslaender/'

write_json_into_raw(mnt_point_RKI_Corona_Bundeslaender_Ingestion, mnt_point_RKI_Corona_Bundeslaender_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Corona_Bundeslaender
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_RKI_Corona_Bundeslaender/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_RKI_Corona_Bundeslaender

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_Data_Status**

# COMMAND ----------

mnt_point_RKI_Data_Status_Ingestion = '/mnt/kaggle/Covid/Ingestion/RKI_Data_Status/'
mnt_point_RKI_Data_Status_Raw = '/mnt/kaggle/Covid/Raw/TBL_RKI_Data_Status/'

write_json_into_raw(mnt_point_RKI_Data_Status_Ingestion, mnt_point_RKI_Data_Status_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_Data_Status
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_RKI_Data_Status/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_RKI_Data_Status

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_key_data**

# COMMAND ----------

mnt_point_RKI_key_data_Ingestion = '/mnt/kaggle/Covid/Ingestion/RKI_key_data/'
mnt_point_RKI_key_data_Raw = '/mnt/kaggle/Covid/Raw/TBL_RKI_key_data/'

write_json_into_raw(mnt_point_RKI_key_data_Ingestion, mnt_point_RKI_key_data_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_key_data
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_RKI_key_data/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_RKI_key_data

# COMMAND ----------

# MAGIC %md
# MAGIC **RKI_history**

# COMMAND ----------

mnt_point_RKI_history_Ingestion = '/mnt/kaggle/Covid/Ingestion/RKI_history/'
mnt_point_RRKI_history_Raw = '/mnt/kaggle/Covid/Raw/TBL_RKI_history/'

write_json_into_raw(mnt_point_RKI_history_Ingestion, mnt_point_RRKI_history_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_RAW.TBL_RKI_history
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/kaggle/Covid/Raw/TBL_RKI_history/'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_RKI_history

# COMMAND ----------


