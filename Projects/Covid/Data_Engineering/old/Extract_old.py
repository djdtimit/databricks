# Databricks notebook source
# MAGIC %run /Repos/tim.stemke@initions-consulting.com/databricks/Projects/Covid/Utilities

# COMMAND ----------

# import os
# from datetime import datetime

# statinfo = os.stat('/dbfs/mnt/kaggle/Covid/Bronze/covid_19_world_vaccination_progress/country_vaccinations.csv')
# modified_date = datetime.fromtimestamp(statinfo.st_mtime)
# print(modified_date)

# COMMAND ----------

interface = kaggle_interface(dbutils.secrets.get('KAGGLE', 'KAGGLE_USERNAME'), dbutils.secrets.get('KAGGLE', 'KAGGLE_KEY'))
interface.download_dataset('gpreda/covid-world-vaccination-progress', 'mnt/kaggle/Covid/Ingestion/covid_19_world_vaccination_progress/', False)
interface.download_dataset('headsortails/covid19-tracking-germany', 'mnt/kaggle/Covid/Ingestion/covid19-tracking-germany/', False)
interface.download_dataset('josephassaker/covid19-global-dataset', 'mnt/kaggle/Covid/Ingestion/covid19-global-dataset/', False)

# COMMAND ----------


