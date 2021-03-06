# Databricks notebook source
import pandas as pd
import urllib
import os
import requests
import re
import databricks.koalas as ks

# COMMAND ----------

def get_rki_data(url, mnt_path, file_name):
  if not os.path.isdir(mnt_path):
    os.makedirs(mnt_path)
  target_path = os.path.join('/dbfs', mnt_path, file_name)
  urllib.request.urlretrieve(url, target_path)
  print('source: ', url)
  print('target: ', target_path)


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

save_path_germany_vaccinations_timeseries_v2 = '/mnt/kaggle/Covid/Ingestion/germany_vaccinations_timeseries_v2/'
save_path_germany_deliveries_timeseries_v2 = '/mnt/kaggle/Covid/Ingestion/germany_deliveries_timeseries_v2/'
save_path_germany_vaccinations_by_state_v1 = '/mnt/kaggle/Covid/Ingestion/germany_vaccinations_by_state_v1/'

# COMMAND ----------

# file_name_vaccinations_timeseries_v2 = 'germany_vaccinations_timeseries_v2.tsv'
# file_name_deliveries_timeseries_v2 = 'germany_deliveries_timeseries_v2.tsv'
# file_name_vaccinations_by_state_v1 = 'germany_vaccinations_by_state.tsv'

# COMMAND ----------

# get_rki_data(url_germany_vaccinations_timeseries_v2, save_path_germany_vaccinations_timeseries_v2, file_name_vaccinations_timeseries_v2)
# get_rki_data(url_germany_deliveries_timeseries_v2, save_path_germany_deliveries_timeseries_v2, file_name_deliveries_timeseries_v2)
# get_rki_data(url_germany_vaccinations_by_state_v1, save_path_germany_vaccinations_by_state_v1, file_name_vaccinations_by_state_v1)

# COMMAND ----------

df = spark.createDataFrame(pd.read_csv(url_germany_vaccinations_timeseries_v2,sep='\t',header=0))
df.write.format('csv').option('sep', ';').option('header', True).mode('overwrite').save(save_path_germany_vaccinations_timeseries_v2)

# COMMAND ----------

df = spark.createDataFrame(pd.read_csv(url_germany_deliveries_timeseries_v2,sep='\t',header=0))
df.write.format('csv').option('sep', ';').option('header', True).mode('overwrite').save(save_path_germany_deliveries_timeseries_v2)

# COMMAND ----------

df = spark.createDataFrame(pd.read_csv(url_germany_vaccinations_by_state_v1,sep='\t',header=0))
df.write.format('csv').option('sep', ';').option('header', True).mode('overwrite').save(save_path_germany_vaccinations_by_state_v1)

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

save_path_csse_covid_19_daily_reports = '/dbfs/mnt/kaggle/Covid/Ingestion/csse_covid_19_daily_reports/'

# COMMAND ----------

counter = 0
for csv_file in csv_files:
  file_url = os.path.join(url.replace('github.com','raw.githubusercontent.com').replace('tree', ''), csv_file)
  
  df_csv_file = pd.read_csv(file_url,sep=',',header=0,dtype=str)
#   df_csv_file['file_name'] = csv_file
  
#   df_csv_file.columns = (df_csv_file.columns.str.replace(" ", "_").str.replace("/", "_").str.replace("Long_","Long").str.replace('Latitude', 'Lat'). 
#   str.replace('Longitude', 'Long').str.replace('Incident_Rate', 'Incidence_Rate').str.replace('-','_'))

  print('Progress: ', counter / len(csv_files) * 100,'%')
  counter += 1
  
  if not os.path.isdir(save_path_csse_covid_19_daily_reports):
    os.makedirs(save_path_csse_covid_19_daily_reports)
    
  target_path = os.path.join(save_path_csse_covid_19_daily_reports, csv_file)
  print(target_path)
  
  df_csv_file.to_csv(target_path, index=False)

# COMMAND ----------


