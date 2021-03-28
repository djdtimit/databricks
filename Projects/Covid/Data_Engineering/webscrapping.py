# Databricks notebook source
import requests
from bs4 import BeautifulSoup

# COMMAND ----------

url = 'https://www.worldometers.info/coronavirus/'

# COMMAND ----------

req = requests.get(url)
bs_obj = BeautifulSoup(req.text, "html.parser")

# COMMAND ----------

bs_obj

# COMMAND ----------


