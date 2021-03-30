# Databricks notebook source
import requests
import json
from bs4 import BeautifulSoup
import pandas as pd

# COMMAND ----------

url = 'https://www.worldometers.info/coronavirus/'

# COMMAND ----------

req = requests.get(url)
bs_obj = BeautifulSoup(req.text, "html.parser")

# COMMAND ----------

bs_obj

# COMMAND ----------

payload = {'code': 'DE'}
#{'country': 'Germany'} # or 
URL = 'https://api.statworx.com/covid'
response = requests.post(url=URL, data=json.dumps(payload))

# Convert to data frame
df = pd.DataFrame.from_dict(json.loads(response.text))

# COMMAND ----------

df.sort_values(by='date')

# COMMAND ----------

url = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1=1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=true&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token='

# COMMAND ----------

response = requests.get(url)
response_data = json.loads(response.content)
covid_counts = response_data['count']

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

df = df.withColumn("Altersgruppe", col("attributes").getItem("Altersgruppe")) \
        .withColumn("Datenstand", col("attributes").getItem("Datenstand")) \
        .withColumn("AnzahlFall", col("attributes").getItem("AnzahlFall")) \
        .withColumn("ObjectId", col("attributes").getItem("ObjectId")) \
        .withColumn("NeuerTodesfall", col("attributes").getItem("NeuerTodesfall")) \
        .withColumn("Landkreis", col("attributes").getItem("Landkreis")) \
        .withColumn("Refdatum", col("attributes").getItem("Refdatum")) \
        .withColumn("NeuGenesen", col("attributes").getItem("NeuGenesen")) \
        .withColumn("AnzahlGenesen", col("attributes").getItem("AnzahlGenesen")) \
        .withColumn("Meldedatum", col("attributes").getItem("Meldedatum")) \
        .withColumn("Bundesland", col("attributes").getItem("Bundesland")) \
        .withColumn("NeuerFall", col("attributes").getItem("NeuerFall")) \
        .withColumn("IstErkrankungsbeginn", col("attributes").getItem("IstErkrankungsbeginn")) \
        .withColumn("IdLandkreis", col("attributes").getItem("IdLandkreis")) \
        .withColumn("Altersgruppe2", col("attributes").getItem("Altersgruppe2")) \
        .withColumn("AnzahlTodesfall", col("attributes").getItem("AnzahlTodesfall")) \
        .withColumn("Geschlecht", col("attributes").getItem("Geschlecht")) \
        .withColumn("IdBundesland", col("attributes").getItem("IdBundesland"))

# COMMAND ----------

df.select('ObjectId').orderBy('ObjectId', ascending=False).show()
