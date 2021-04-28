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

# COMMAND ----------

get_vaccinations_data(url_germany_deliveries_timeseries_v2, tmp_path_germany_deliveries_timeseries_v2, save_path_germany_deliveries_timeseries_v2)

# COMMAND ----------

get_vaccinations_data(url_germany_vaccinations_by_state_v1, tmp_path_germany_vaccinations_by_state_v1, save_path_germany_vaccinations_by_state_v1)

# COMMAND ----------

url = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports/'
r = requests.get(url)

# COMMAND ----------

html_doc = r.text

# COMMAND ----------

import regex as re

# COMMAND ----------

file = re.findall(r'[^(=")]*?csv', html_doc)

# COMMAND ----------

def fun(variable):
  chars = ['>', '/']
  if any((c in chars) for c in variable):
    return False
  else:
    return True

# COMMAND ----------

csv_files = list(filter(fun, file))

# COMMAND ----------

csv_files

# COMMAND ----------

save_path_csse_covid_19_daily_reports = 'dbfs:/mnt/covid/Ingestion/csse_covid_19_daily_reports/'
tmp_path_csse_covid_19_daily_reports = 'dbfs:/tmp/'

# COMMAND ----------

g = Github(dbutils.secrets.get('Github', 'csse_covid_19'))
repo_name = "CSSEGISandData/COVID-19"
repo_path = "csse_covid_19_data/csse_covid_19_daily_reports/"
repo = g.get_repo(repo_name)

df_load_history = spark.sql('SELECT count(*) as c, MAX(last_load_ts) as last_load_ts from COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history').collect()

load_history_count = df_load_history[0].c


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

    print('target_path: ', target_path)
    
    spark.sql(f"""INSERT INTO COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history VALUES ('{csv_file}','{last_commit_message}',from_utc_timestamp('{current_ts}', 'Europe/Berlin'))""")

  
print('Number of loaded files: ', counter)

spark.sql("""Optimize COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS COVID_INGESTION

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history (
# MAGIC file_name STRING,
# MAGIC last_commit_message STRING,
# MAGIC last_load_ts TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/covid/Ingestion/TBL_csse_covid_19_daily_reports_load_history/'

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history
