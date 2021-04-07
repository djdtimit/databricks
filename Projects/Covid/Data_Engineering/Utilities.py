# Databricks notebook source
import os

class kaggle_interface:
  
  def __init__(self, user_name, key):
    self.user_name = user_name
    self.key = key
    os.environ['KAGGLE_USERNAME'] = self.user_name
    os.environ['KAGGLE_KEY'] = self.key
    from kaggle.api.kaggle_api_extended import KaggleApi
    self.api = KaggleApi()
    self.api.authenticate()
  
  def download_dataset(self, dataset_name, destination, backup = True):
    self.api.dataset_download_files(dataset_name, path = '/dbfs/tmp/', force = True)
    self.__create_target_folder(destination)
    self.__unzip_into_target(dataset_name, destination)
    if backup:
      os.system(f"""unzip -o /dbfs/tmp/{dataset_name.split('/')[1]}.zip -d /dbfs/{destination}{self.__get_dataset_metadata(dataset_name).strftime("%Y-%m-%d")}/""")
    
  def __get_dataset_metadata(self, dataset_name):
    datasets=self.api.dataset_list(search=dataset_name)
    dataset_info = {str(dataset):dataset.lastUpdated for dataset in datasets}
    return dataset_info[dataset_name]
  
  def __create_target_folder(self, destination):
    dbutils.fs.mkdirs(destination)
    
    
  def __unzip_into_target(self, dataset_name, destination):
    os.system(f"""unzip -o /dbfs/tmp/{dataset_name.split('/')[1]}.zip -d /dbfs/{destination}""")

# COMMAND ----------

from datetime import date

class data_lake:
  
  def __init__(self, archive_schema = '', ingestion_schema = '', raw_schema = '', qualified_schema = '', curated_schema = ''):
    self.archive_schema = archive_schema
    self.ingestion_schema = ingestion_schema
    self.raw_schema = raw_schema
    self.qualified_schema = qualified_schema
    self.curated_schema = curated_schema
    
    if self.archive_schema != '':
      self.archive_views = spark.sql(f"show views in {self.archive_schema}").select('namespace', 'viewName').collect()
    if self.ingestion_schema != '':
      self.ingestion_views = spark.sql(f"show views in {self.ingestion_schema}").select('namespace', 'viewName').collect()
    if self.raw_schema != '':
      self.raw_views = spark.sql(f"show views in {self.raw_schema}").select('namespace', 'viewName').collect()
    if self.qualified_schema != '':
      self.qualified_views = spark.sql(f"show views in {self.qualified_schema}").select('namespace', 'viewName').collect()
    if self.curated_schema != '':
      self.curated_views = spark.sql(f"show views in {self.curated_schema}").select('namespace', 'viewName').collect()
    
  def mv_ingestion_to_archive(self):
    for raw_view in self.raw_views:
      df_source = self.__read_source_table(raw_view)
      self.__mv_files(df_source)
      
  def __read_source_table(self, view_name):
    df_source = spark.read.table(f"""{view_name['namespace']}.{view_name['viewName']}""").select('source').distinct().collect()
    return df_source
  
  def __mv_files(self, df_source):
    today = date.today()
    dbutils.fs.mv(df_source[0]['source'], df_source[0]['source'].replace('/Ingestion/', f'/Archive/{today}/'), recurse=True)

# COMMAND ----------

# import os
# from datetime import datetime

# statinfo = os.stat('/dbfs/mnt/kaggle/Covid/Bronze/covid_19_world_vaccination_progress/country_vaccinations.csv')
# modified_date = datetime.fromtimestamp(statinfo.st_mtime)
# print(modified_date)
