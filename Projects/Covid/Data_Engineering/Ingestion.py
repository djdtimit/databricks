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

def get_rki_data(url, mnt_path, file_name):
  if not os.path.isdir(mnt_path):
    os.makedirs(mnt_path)
  target_path = os.path.join('/dbfs', mnt_path, file_name)
  urllib.request.urlretrieve(url, target_path)
  print('source: ', url)
  print('target: ', target_path)


# COMMAND ----------

def get_vaccinations_data(url, save_mnt_path):
  df = pd.read_csv(url,sep='\t',header=0,dtype=str)
  if not os.path.isdir(save_mnt_path.rsplit(sep='/',maxsplit=1)[0]):
    os.makedirs(save_mnt_path.rsplit(sep='/',maxsplit=1)[0])
    
  df.to_csv(save_mnt_path, index=False)

# COMMAND ----------

url_RKI_COVID19 = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.geojson'
url_RKI_Corona_Landkreise = 'https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.geojson'
url_RKI_Corona_Bundeslaender = 'https://opendata.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0.geojson'
url_RKI_Data_Status = 'https://opendata.arcgis.com/datasets/3a12c19fb209431e85dda38f59aa16ba_0.geojson'
url_RKI_Altersgruppen = 'https://opendata.arcgis.com/datasets/23b1ccb051f543a5b526021275c1c6e5_0.geojson'
url_RKI_key_data = 'https://opendata.arcgis.com/datasets/c2f3c3b935a242169c6bec82e1fa573e_0.geojson'
url_RKI_history = 'https://opendata.arcgis.com/datasets/6d78eb3b86ad4466a8e264aa2e32a2e4_0.geojson'

# COMMAND ----------

save_path_RKI_COVID19 = '/mnt/kaggle/Covid/Ingestion/RKI_COVID19/'
save_path_RKI_Corona_Landkreise = '/mnt/kaggle/Covid/Ingestion/RKI_Corona_Landkreise/'
save_path_RKI_Corona_Bundeslaender = '/mnt/kaggle/Covid/Ingestion/RKI_Corona_Bundeslaender/'
save_path_RKI_Data_Status = '/mnt/kaggle/Covid/Ingestion/RKI_Data_Status/'
save_path_RKI_Altersgruppen = '/mnt/kaggle/Covid/Ingestion/RKI_Altersgruppen/'
save_path_RKI_key_data = '/mnt/kaggle/Covid/Ingestion/RKI_key_data/'
save_path_RKI_history = '/mnt/kaggle/Covid/Ingestion/RKI_history/'

# COMMAND ----------

file_name_RKI_COVID19 = 'RKI_COVID19.json'
file_name_RKI_Corona_Landkreise = 'RKI_Corona_Landkreise.json'
file_name_RKI_Corona_Bundeslaender = 'RKI_Corona_Bundeslaender.json'
file_name_RKI_Data_Status = 'RKI_Data_Status.json'
file_name_RKI_Altersgruppen = 'RKI_Altersgruppen.json'
file_name_RKI_key_data = 'RKI_key_data.json'
file_name_RKI_history = 'RKI_history.json'

# COMMAND ----------

get_rki_data(url_RKI_COVID19, save_path_RKI_COVID19, file_name_RKI_COVID19)
get_rki_data(url_RKI_Corona_Landkreise, save_path_RKI_Corona_Landkreise, file_name_RKI_Corona_Landkreise)
get_rki_data(url_RKI_Corona_Bundeslaender, save_path_RKI_Corona_Bundeslaender, file_name_RKI_Corona_Bundeslaender)
get_rki_data(url_RKI_Data_Status, save_path_RKI_Data_Status, file_name_RKI_Data_Status)
get_rki_data(url_RKI_Altersgruppen, save_path_RKI_Altersgruppen, file_name_RKI_Altersgruppen)
get_rki_data(url_RKI_key_data, save_path_RKI_key_data, file_name_RKI_key_data)
get_rki_data(url_RKI_history, save_path_RKI_history, file_name_RKI_history)

# COMMAND ----------

url_germany_vaccinations_timeseries_v2 = 'https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv'
url_germany_deliveries_timeseries_v2 = 'https://impfdashboard.de/static/data/germany_deliveries_timeseries_v2.tsv'
url_germany_vaccinations_by_state_v1 = 'https://impfdashboard.de/static/data/germany_vaccinations_by_state.tsv'

# COMMAND ----------

save_path_germany_vaccinations_timeseries_v2 = '/dbfs/mnt/kaggle/Covid/Ingestion/germany_vaccinations_timeseries_v2/germany_vaccinations_timeseries_v2.csv'
save_path_germany_deliveries_timeseries_v2 = '/dbfs/mnt/kaggle/Covid/Ingestion/germany_deliveries_timeseries_v2/germany_deliveries_timeseries_v2.csv'
save_path_germany_vaccinations_by_state_v1 = '/dbfs/mnt/kaggle/Covid/Ingestion/germany_vaccinations_by_state_v1/germany_vaccinations_by_state_v1.csv'

# COMMAND ----------

get_vaccinations_data(url_germany_vaccinations_timeseries_v2, save_path_germany_vaccinations_timeseries_v2)

# COMMAND ----------

get_vaccinations_data(url_germany_deliveries_timeseries_v2, save_path_germany_deliveries_timeseries_v2)

# COMMAND ----------

get_vaccinations_data(url_germany_vaccinations_by_state_v1, save_path_germany_vaccinations_by_state_v1)

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

save_path_csse_covid_19_daily_reports = '/dbfs/mnt/kaggle/Covid/Ingestion/csse_covid_19_daily_reports/'

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

    if not os.path.isdir(save_path_csse_covid_19_daily_reports):
      os.makedirs(save_path_csse_covid_19_daily_reports)

    target_path = os.path.join(save_path_csse_covid_19_daily_reports, csv_file)
    
    df_csv_file.to_csv(target_path, index=False)
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
# MAGIC LOCATION '/mnt/kaggle/Covid/Ingestion/TBL_csse_covid_19_daily_reports_load_history/'

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE table COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM COVID_INGESTION.TBL_csse_covid_19_daily_reports_load_history

# COMMAND ----------


