# Databricks notebook source
import pandas as pd
import urllib
import os
import requests
import re
import databricks.koalas as ks
from github import Github
import datetime
import pytz
import regex as re
import databricks.koalas as ks
from pyspark.sql.functions import input_file_name, current_timestamp, from_utc_timestamp, col

# COMMAND ----------

connectionString = dbutils.secrets.get(scope = "RS-TSE-KV", key = 'tselabConnectionString')
resourceGroup = dbutils.secrets.get(scope = "RS-TSE-KV", key = 'ressourceGroup')
subscriptionId = dbutils.secrets.get(scope = "RS-TSE-KV", key = 'subscriptionId')
tenantId = dbutils.secrets.get(scope = "RS-TSE-KV", key = 'tenantId')
clientId = dbutils.secrets.get(scope = "RS-TSE-KV", key = 'clientId')
clientSecret = dbutils.secrets.get(scope = "RS-TSE-KV", key = 'clientSecret')

# COMMAND ----------

container_name = 'covid'
storage_account_name = 'tselab'
mount_name = 'covid/'

# COMMAND ----------

def get_rki_data(url, tmp_path, save_mnt_path):
  urllib.request.urlretrieve(url, tmp_path.replace('dbfs:/','/dbfs/'))
  dbutils.fs.mv(tmp_path, save_mnt_path, True)

# COMMAND ----------

def get_vaccinations_data(url, tmp_path, save_mnt_path):
  df = pd.read_csv(url,sep='\t',header=0,dtype=str)
  df.to_csv(tmp_path.replace('dbfs:/','/dbfs/'), index=False)
  dbutils.fs.mv(tmp_path, save_mnt_path, True)


# COMMAND ----------

def fun(variable):
  chars = ['>', '/']
  if any((c in chars) for c in variable):
    return False
  else:
    return True

# COMMAND ----------

def write_csv_into_raw(ingestion_load_path, raw_save_path):
  df = ks.read_csv(ingestion_load_path,dtype=str)
  df['_source'] = input_file_name()
  df['_insert_TS'] = from_utc_timestamp(current_timestamp(), 'Europe/Berlin')
  df.to_delta(raw_save_path,mode='overwrite', index=False)

# COMMAND ----------

def write_json_into_raw(ingestion_load_path, raw_save_path):
  df = ks.read_json(ingestion_load_path,dtype=str)
  df['_source'] = input_file_name()
  df['_insert_TS'] = from_utc_timestamp(current_timestamp(), 'Europe/Berlin')
  df.to_delta(raw_save_path,mode='overwrite', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion

# COMMAND ----------

url_RKI_COVID19 = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.geojson'
url_RKI_Corona_Landkreise = 'https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.geojson'
url_RKI_Corona_Bundeslaender = 'https://opendata.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0.geojson'
url_RKI_Data_Status = 'https://opendata.arcgis.com/datasets/3a12c19fb209431e85dda38f59aa16ba_0.geojson'
url_RKI_Altersgruppen = 'https://opendata.arcgis.com/datasets/23b1ccb051f543a5b526021275c1c6e5_0.geojson'
url_RKI_key_data = 'https://opendata.arcgis.com/datasets/c2f3c3b935a242169c6bec82e1fa573e_0.geojson'
url_RKI_history = 'https://opendata.arcgis.com/datasets/6d78eb3b86ad4466a8e264aa2e32a2e4_0.geojson'

# COMMAND ----------

tmp_path_RKI_COVID19 = 'dbfs:/tmp/RKI_COVID19.json'
tmp_path_RKI_Corona_Landkreise = 'dbfs:/tmp/RKI_Corona_Landkreise.json'
tmp_path_RKI_Corona_Bundeslaender = 'dbfs:/tmp/RKI_Corona_Bundeslaender.json'
tmp_path_RKI_Data_Status = 'dbfs:/tmp/RKI_Data_Status.json'
tmp_path_RKI_Altersgruppen = 'dbfs:/tmp/RKI_Altersgruppen.json'
tmp_path_RKI_key_data = 'dbfs:/tmp/RKI_key_data.json'
tmp_path_RKI_history = 'dbfs:/tmp/RKI_history.json'

# COMMAND ----------

save_path_RKI_COVID19 = 'dbfs:/mnt/covid/Ingestion/RKI_COVID19/RKI_COVID19.json'
save_path_RKI_Corona_Landkreise = 'dbfs:/mnt/covid/Ingestion/RKI_Corona_Landkreise/RKI_Corona_Landkreise.json'
save_path_RKI_Corona_Bundeslaender = 'dbfs:/mnt/covid/Ingestion/RKI_Corona_Bundeslaender/RKI_Corona_Bundeslaender.json'
save_path_RKI_Data_Status = 'dbfs:/mnt/covid/Ingestion/RKI_Data_Status/RKI_Data_Status.json'
save_path_RKI_Altersgruppen = 'dbfs:/mnt/covid/Ingestion/RKI_Altersgruppen/RKI_Altersgruppen.json'
save_path_RKI_key_data = 'dbfs:/mnt/covid/Ingestion/RKI_key_data/RKI_key_data.json'
save_path_RKI_history = 'dbfs:/mnt/covid/Ingestion/RKI_history/RKI_history.json'

# COMMAND ----------

get_rki_data(url_RKI_COVID19, tmp_path_RKI_COVID19, save_path_RKI_COVID19)
get_rki_data(url_RKI_Corona_Landkreise, tmp_path_RKI_Corona_Landkreise, save_path_RKI_Corona_Landkreise)
get_rki_data(url_RKI_Corona_Bundeslaender, tmp_path_RKI_Corona_Bundeslaender, save_path_RKI_Corona_Bundeslaender)
get_rki_data(url_RKI_Data_Status, tmp_path_RKI_Data_Status, save_path_RKI_Data_Status)
get_rki_data(url_RKI_Altersgruppen, tmp_path_RKI_Altersgruppen, save_path_RKI_Altersgruppen)
get_rki_data(url_RKI_key_data, tmp_path_RKI_key_data, save_path_RKI_key_data)
get_rki_data(url_RKI_history, tmp_path_RKI_history, save_path_RKI_history)

# COMMAND ----------

url_germany_vaccinations_timeseries_v2 = 'https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv'
url_germany_deliveries_timeseries_v2 = 'https://impfdashboard.de/static/data/germany_deliveries_timeseries_v2.tsv'
url_germany_vaccinations_by_state_v1 = 'https://impfdashboard.de/static/data/germany_vaccinations_by_state.tsv'

# COMMAND ----------

tmp_path_germany_vaccinations_timeseries_v2 = 'dbfs:/tmp/germany_vaccinations_timeseries_v2.csv'
tmp_path_germany_deliveries_timeseries_v2 = 'dbfs:/tmp/germany_deliveries_timeseries_v2.csv'
tmp_path_germany_vaccinations_by_state_v1 = 'dbfs:/tmp/germany_vaccinations_by_state_v1.csv'

# COMMAND ----------

save_path_germany_vaccinations_timeseries_v2 = 'dbfs:/mnt/covid/Ingestion/germany_vaccinations_timeseries_v2/germany_vaccinations_timeseries_v2.csv'
save_path_germany_deliveries_timeseries_v2 = 'dbfs:/mnt/covid/Ingestion/germany_deliveries_timeseries_v2/germany_deliveries_timeseries_v2.csv'
save_path_germany_vaccinations_by_state_v1 = 'dbfs:/mnt/covid/Ingestion/germany_vaccinations_by_state_v1/germany_vaccinations_by_state_v1.csv'

# COMMAND ----------

get_vaccinations_data(url_germany_vaccinations_timeseries_v2, tmp_path_germany_vaccinations_timeseries_v2, save_path_germany_vaccinations_timeseries_v2)
get_vaccinations_data(url_germany_deliveries_timeseries_v2, tmp_path_germany_deliveries_timeseries_v2, save_path_germany_deliveries_timeseries_v2)
get_vaccinations_data(url_germany_vaccinations_by_state_v1, tmp_path_germany_vaccinations_by_state_v1, save_path_germany_vaccinations_by_state_v1)

# COMMAND ----------

url = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports/'
r = requests.get(url)
html_doc = r.text
file = re.findall(r'[^(=")]*?csv', html_doc)
csv_files = list(filter(fun, file))
save_path_csse_covid_19_daily_reports = 'dbfs:/mnt/covid/Ingestion/csse_covid_19_daily_reports/'
tmp_path_csse_covid_19_daily_reports = 'dbfs:/tmp/'
g = Github(dbutils.secrets.get('Github', 'csse_covid_19'))
repo_name = "CSSEGISandData/COVID-19"
repo_path = "csse_covid_19_data/csse_covid_19_daily_reports/"
repo = g.get_repo(repo_name)

df_load_history = spark.sql('SELECT count(*) as c, MAX(last_load_ts) as last_load_ts from COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history').collect()

load_history_count = df_load_history[0].c

loaded_csv_files = []
last_commit_messages = []


if load_history_count == 0:
  last_load_ts = datetime.datetime(2019, 4, 25)
else:
  last_load_ts = df_load_history[0].last_load_ts

current_ts = datetime.datetime.now(pytz.timezone('Europe/Berlin'))

counter = 0
for csv_file in csv_files:
  
  commits = repo.get_commits(path=os.path.join(repo_path, csv_file))
  last_commit_ts = commits[0].commit.committer.date
  last_commit_message = commits[0].commit.message
    
  if last_load_ts < last_commit_ts:
  
    file_url = os.path.join(url.replace('github.com','raw.githubusercontent.com').replace('tree', ''), csv_file)

    df_csv_file = pd.read_csv(file_url,sep=',',header=0,dtype=str)

    counter += 1

    target_path = os.path.join(save_path_csse_covid_19_daily_reports, csv_file)
    
    df_csv_file.to_csv(os.path.join(tmp_path_csse_covid_19_daily_reports.replace('dbfs:/','/dbfs/'), csv_file), index=False)
    dbutils.fs.mv(os.path.join(tmp_path_csse_covid_19_daily_reports, csv_file), target_path, True)
    
    loaded_csv_files.append(csv_file)
    last_commit_messages.append(last_commit_message)

    print('target_path: ', target_path)

# gather the load information in one dataframe at the end
df = pd.DataFrame(list(zip(loaded_csv_files, last_commit_messages)),
               columns =['file_name', 'last_commit_message'])
df['last_load_ts'] = current_ts.strftime("%Y-%m-%d %H:%M:%S")
df_history_insert = spark.createDataFrame(df)
df_history_insert.createOrReplaceTempView('history_insert')
# ... and insert into load history table to decrease insert time of every single loaded file into this table
spark.sql("INSERT INTO COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history SELECT file_name, last_commit_message,from_utc_timestamp(last_load_ts, 'Europe/Berlin') FROM history_insert")  

print('Number of loaded files: ', counter)

spark.sql("""Optimize COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.useNotifications", "true")
      .option("cloudFiles.allowOverwrites", "true")
      .option("cloudFiles.connectionString", connectionString)
      .option("cloudFiles.resourceGroup", resourceGroup)
      .option("cloudFiles.subscriptionId", subscriptionId)
      .option("cloudFiles.tenantId", tenantId)
      .option("cloudFiles.clientId", clientId)
      .option("cloudFiles.clientSecret", clientSecret)
      .option("cloudFiles.schemaLocation", '/mnt/covid/Ingestion/csse_covid_19_daily_reports/')
      .option("cloudFiles.format", "text")
      .load('/mnt/covid/Ingestion/csse_covid_19_daily_reports/')
     .withColumn('_source', input_file_name()).withColumn('_insert_TS', from_utc_timestamp(current_timestamp(), 'Europe/Berlin')))


# COMMAND ----------

(df.writeStream.format("delta") 
 .option("mergeSchema", "true")
  .option("checkpointLocation", '/mnt/covid/Raw/TBL_RKI_COVID19_AL/checkpoint/' )
  .option("cloudFiles.useNotifications", True)
  .trigger(once=True)
  .start('/mnt/covid/Raw/TBL_RKI_COVID19_AL/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/mnt/covid/Raw/TBL_RKI_COVID19_AL/`

# COMMAND ----------

df_csse_covid_19_daily_reports = (spark.read.option('header',True).option('sep',',').csv('dbfs:/mnt/covid/Ingestion/csse_covid_19_daily_reports/').withColumn('_source', input_file_name()).withColumn('_insert_TS', from_utc_timestamp(current_timestamp(), 'Europe/Berlin')))

# COMMAND ----------

df_csse_covid_19_daily_reports.write.format("delta") \
           .option("mergeSchema", "true") \
           .mode("append") \
           .save('dbfs:/mnt/covid/Raw/TBL_csse_covid_19_daily_reports/')

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE COVID_RAW.TBL_csse_covid_19_daily_reports;
# MAGIC VACUUM  COVID_RAW.TBL_csse_covid_19_daily_reports;

# COMMAND ----------

mnt_point_germany_vaccinations_timeseries_v2_Ingestion = '/mnt/covid/Ingestion/germany_vaccinations_timeseries_v2/'
mnt_point_germany_vaccinations_timeseries_v2_Raw = '/mnt/covid/Raw/TBL_germany_vaccinations_timeseries_v2/'

write_csv_into_raw(mnt_point_germany_vaccinations_timeseries_v2_Ingestion, mnt_point_germany_vaccinations_timeseries_v2_Raw)

# COMMAND ----------

mnt_point_germany_deliveries_timeseries_v2_Ingestion = '/mnt/covid/Ingestion/germany_deliveries_timeseries_v2/'
mnt_point_germany_deliveries_timeseries_v2_Raw = '/mnt/covid/Raw/TBL_germany_deliveries_timeseries_v2/'

write_csv_into_raw(mnt_point_germany_deliveries_timeseries_v2_Ingestion, mnt_point_germany_deliveries_timeseries_v2_Raw)

# COMMAND ----------

mnt_point_germany_vaccinations_by_state_v1_Ingestion = '/mnt/covid/Ingestion/germany_vaccinations_by_state_v1/'
mnt_point_germany_vaccinations_by_state_v1_Raw = '/mnt/covid/Raw/TBL_germany_vaccinations_by_state_v1/'

write_csv_into_raw(mnt_point_germany_vaccinations_by_state_v1_Ingestion, mnt_point_germany_vaccinations_by_state_v1_Raw)

# COMMAND ----------

mnt_point_RKI_Altersgruppen_Ingestion = '/mnt/covid/Ingestion/RKI_Altersgruppen/'
mnt_point_RKI_Altersgruppen_Raw = '/mnt/covid/Raw/TBL_RKI_Altersgruppen/'

write_json_into_raw(mnt_point_RKI_Altersgruppen_Ingestion, mnt_point_RKI_Altersgruppen_Raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM covid_raw.tbl_rki_covid19

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM covid_raw.tbl_rki_covid19

# COMMAND ----------

mnt_point_RKI_COVID19_Ingestion = '/mnt/covid/Ingestion/RKI_COVID19/'
mnt_point_RKI_COVID19_Raw = '/mnt/covid/Raw/TBL_RKI_COVID19/'

write_json_into_raw(mnt_point_RKI_COVID19_Ingestion, mnt_point_RKI_COVID19_Raw)

# COMMAND ----------

mnt_point_RKI_Corona_Landkreise_Ingestion = '/mnt/covid/Ingestion/RKI_Corona_Landkreise/'
mnt_point_RKI_Corona_Landkreise_Raw = '/mnt/covid/Raw/TBL_RKI_Corona_Landkreise/'

write_json_into_raw(mnt_point_RKI_Corona_Landkreise_Ingestion, mnt_point_RKI_Corona_Landkreise_Raw)

# COMMAND ----------

mnt_point_RKI_Corona_Bundeslaender_Ingestion = '/mnt/covid/Ingestion/RKI_Corona_Bundeslaender/'
mnt_point_RKI_Corona_Bundeslaender_Raw = '/mnt/covid/Raw/TBL_RKI_Corona_Bundeslaender/'

write_json_into_raw(mnt_point_RKI_Corona_Bundeslaender_Ingestion, mnt_point_RKI_Corona_Bundeslaender_Raw)

# COMMAND ----------

mnt_point_RKI_Data_Status_Ingestion = '/mnt/covid/Ingestion/RKI_Data_Status/'
mnt_point_RKI_Data_Status_Raw = '/mnt/covid/Raw/TBL_RKI_Data_Status/'

write_json_into_raw(mnt_point_RKI_Data_Status_Ingestion, mnt_point_RKI_Data_Status_Raw)

# COMMAND ----------

mnt_point_RKI_key_data_Ingestion = '/mnt/covid/Ingestion/RKI_key_data/'
mnt_point_RKI_key_data_Raw = '/mnt/covid/Raw/TBL_RKI_key_data/'

write_json_into_raw(mnt_point_RKI_key_data_Ingestion, mnt_point_RKI_key_data_Raw)

# COMMAND ----------

mnt_point_RKI_history_Ingestion = '/mnt/covid/Ingestion/RKI_history/'
mnt_point_RRKI_history_Raw = '/mnt/covid/Raw/TBL_RKI_history/'

write_json_into_raw(mnt_point_RKI_history_Ingestion, mnt_point_RRKI_history_Raw)

# COMMAND ----------

# MAGIC %md
# MAGIC # Archive

# COMMAND ----------

database_objects = spark.sql("show tables in covid_raw").select('database', 'tableName').where(col('isTemporary') == 'false').collect()
for database_object in database_objects:
  database_name = database_object['database']
  table_name = database_object['tableName']
  paths = spark.sql(f"""SELECT distinct _source as PATH FROM {database_name}.{table_name}""").collect()
  source_mnt_path = paths[0]['PATH'].rsplit('/',1)[0]
  
  listed_files_paths = [listed_file[0] for listed_file in dbutils.fs.ls(source_mnt_path)]
    
  for path in paths:
    source_path = path['PATH']
    target_path = path['PATH'].replace('/Ingestion/', '/Archive/')
    
    # check if files in data lake do exist
    if source_path in listed_files_paths:
      dbutils.fs.mv(source_path, target_path)
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Qualified

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO covid_qualified.TBL_csse_covid_19_daily_reports AS T USING covid_qualified.VW_csse_covid_19_daily_reports AS S 
# MAGIC ON (
# MAGIC   T.ADMIN2 = S.ADMIN2
# MAGIC   OR (
# MAGIC     T.ADMIN2 IS NULL
# MAGIC     AND S.ADMIN2 IS NULL
# MAGIC   )
# MAGIC )
# MAGIC AND (
# MAGIC   T.Province_State = S.Province_State
# MAGIC   OR (
# MAGIC     T.Province_State IS NULL
# MAGIC     AND S.Province_State IS NULL
# MAGIC   )
# MAGIC )
# MAGIC AND T.Country_Region = S.Country_Region
# MAGIC AND T.last_update = S.last_update
# MAGIC WHEN MATCHED
# MAGIC AND datediff(CURRENT_TIMESTAMP, S._INSERT_TS) <= 14 THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE covid_qualified.TBL_csse_covid_19_daily_reports;
# MAGIC VACUUM  covid_qualified.TBL_csse_covid_19_daily_reports;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_germany_vaccinations_timeseries_v2;
# MAGIC INSERT INTO covid_qualified.TBL_germany_vaccinations_timeseries_v2 SELECT * FROM covid_qualified.VW_germany_vaccinations_timeseries_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_germany_vaccinations_timeseries_v2;
# MAGIC INSERT INTO covid_qualified.TBL_germany_vaccinations_timeseries_v2 SELECT * FROM covid_qualified.VW_germany_vaccinations_timeseries_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_germany_vaccinations_by_state_v1;
# MAGIC INSERT INTO covid_qualified.TBL_germany_vaccinations_by_state_v1 SELECT * FROM covid_qualified.VW_germany_vaccinations_by_state_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_RKI_Altersgruppen;
# MAGIC INSERT INTO covid_qualified.TBL_RKI_Altersgruppen SELECT * FROM covid_qualified.VW_RKI_Altersgruppen;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_RKI_COVID19;
# MAGIC INSERT INTO covid_qualified.TBL_RKI_COVID19 SELECT * FROM covid_qualified.VW_RKI_COVID19;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_RKI_Corona_Landkreise;
# MAGIC INSERT INTO covid_qualified.TBL_RKI_Corona_Landkreise SELECT * FROM covid_qualified.VW_RKI_Corona_Landkreise;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_RKI_Corona_Bundeslaender;
# MAGIC INSERT INTO covid_qualified.TBL_RKI_Corona_Bundeslaender SELECT * FROM covid_qualified.VW_RKI_Corona_Bundeslaender;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE covid_qualified.TBL_RKI_key_data;
# MAGIC INSERT INTO covid_qualified.TBL_RKI_key_data SELECT * FROM covid_qualified.VW_RKI_key_data;

# COMMAND ----------

# MAGIC %md
# MAGIC # Curated

# COMMAND ----------


