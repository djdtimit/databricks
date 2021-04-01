# Databricks notebook source
import requests
import json
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.types import *

# COMMAND ----------

def get_RKI_data(url, schema, save_path):
  response = requests.get(url)
  try:
    response_data = json.loads(response.content)
  except:
    print('Content is not available')
  df = spark.createDataFrame(response_data['features'], schema=schema).drop('type')
  df.write.format('json').mode('overwrite').save(save_path)

# COMMAND ----------

url_RKI_COVID19 = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.geojson'
url_RKI_Corona_Landkreise = 'https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.geojson'
url_RKI_Corona_Bundeslaender = 'https://opendata.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0.geojson'
url_RKI_Data_Status = 'https://opendata.arcgis.com/datasets/3a12c19fb209431e85dda38f59aa16ba_0.geojson'
url_RKI_Altersgruppen = 'https://opendata.arcgis.com/datasets/23b1ccb051f543a5b526021275c1c6e5_0.geojson'
url_RKI_key_data = 'https://opendata.arcgis.com/datasets/c2f3c3b935a242169c6bec82e1fa573e_0.geojson'
url_RKI_history = 'https://opendata.arcgis.com/datasets/6d78eb3b86ad4466a8e264aa2e32a2e4_0.geojson'

# COMMAND ----------

schema_RKI_COVID19 = StructType([
    StructField("type", StringType(), True),
    StructField("properties"
                , StructType([StructField('ObjectId', StringType(), True),
                              StructField('IdBundesland', StringType(), True),
                              StructField('Bundesland', StringType(), True),
                              StructField('Landkreis', StringType(), True),
                              StructField('Altersgruppe', StringType(), True),
                              StructField('Geschlecht', StringType(), True),
                              StructField('AnzahlFall', StringType(), True),
                              StructField('AnzahlTodesfall', StringType(), True),
                              StructField('Meldedatum', StringType(), True),
                              StructField('IdLandkreis', StringType(), True),
                              StructField('Datenstand', StringType(), True),
                              StructField('NeuerFall', StringType(), True),
                              StructField('NeuerTodesfall', StringType(), True),
                              StructField('Refdatum', StringType(), True),
                              StructField('NeuGenesen', StringType(), True),
                              StructField('AnzahlGenesen', StringType(), True),
                              StructField('IstErkrankungsbeginn', StringType(), True),
                              StructField('Altersgruppe2', StringType(), True)])
               )                          
])


schema_RKI_Corona_Landkreise = StructType(
    [
    StructField("type", StringType(), True),
    StructField("properties", StructType([StructField("OBJECTID", StringType(), True),
        StructField("ADE", StringType(), True),
        StructField("GF", StringType(), True),
        StructField("BSG", StringType(), True),
        StructField("RS", StringType(), True),
        StructField("AGS", StringType(), True),
        StructField("SDV_RS", StringType(), True),
        StructField("GEN", StringType(), True),
        StructField("BEZ", StringType(), True),
        StructField("IBZ", StringType(), True),
        StructField("BEM", StringType(), True),
        StructField("NBD", StringType(), True),
        StructField("SN_L", StringType(), True),
        StructField("SN_R", StringType(), True),
        StructField("SN_K", StringType(), True),
        StructField("SN_V1", StringType(), True),
        StructField("SN_V2", StringType(), True),
        StructField("SN_G", StringType(), True),
        StructField("FK_S3", StringType(), True),
        StructField("NUTS", StringType(), True),
        StructField("RS_0", StringType(), True),
        StructField("AGS_0", StringType(), True),
        StructField("WSK", StringType(), True),
        StructField("EWZ", StringType(), True),
        StructField("KFL", StringType(), True),
        StructField("DEBKG_ID", StringType(), True),
        StructField("death_rate", StringType(), True),
        StructField("cases", StringType(), True),
        StructField("deaths", StringType(), True),
        StructField("cases_per_100k", StringType(), True),
        StructField("cases_per_population", StringType(), True),
        StructField("BL", StringType(), True),
        StructField("BL_ID", StringType(), True),
        StructField("county", StringType(), True),
        StructField("last_update", StringType(), True),
        StructField("cases7_per_100k", StringType(), True),
        StructField("recovered", StringType(), True),
        StructField("EWZ_BL", StringType(), True),
        StructField("cases7_bl_per_100k", StringType(), True),
        StructField("cases7_bl", StringType(), True),
        StructField("death7_bl", StringType(), True),
        StructField("cases7_lk", StringType(), True),
        StructField("death7_lk", StringType(), True),
        StructField("cases7_per_100k_txt", StringType(), True),
        StructField("AdmUnitId", StringType(), True),
        StructField("SHAPE_Length", StringType(), True),
        StructField("SHAPE_Area", StringType(), True)
    ]
)
),
StructField("geometry", StructType([StructField("Type", StringType(), True) , StructField('coordinates', ArrayType(StringType()), True)
                                   ]) )
]
)

# COMMAND ----------

save_path_RKI_COVID19 = 'mnt/kaggle/Covid/Ingestion/RKI_COVID19/'
save_path_RKI_Corona_Landkreise = 'mnt/kaggle/Covid/Ingestion/RKI_Corona_Landkreise/'

# COMMAND ----------

get_RKI_data('https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.geojson', schema, 'mnt/kaggle/Covid/Ingestion/RKI_COVID19/')

# COMMAND ----------

get_RKI_data(url_RKI_Corona_Landkreise, schema_RKI_Corona_Landkreise, save_path_RKI_Corona_Landkreise)

# COMMAND ----------

url_germany_vaccinations_timeseries_v2 = 'https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv'
url_germany_deliveries_timeseries_v2 = 'https://impfdashboard.de/static/data/germany_deliveries_timeseries_v2.tsv'
url_germany_vaccinations_by_state_v1 = 'https://impfdashboard.de/static/data/germany_vaccinations_by_state.tsv'

# COMMAND ----------

save_path_germany_vaccinations_timeseries_v2 = 'mnt/kaggle/Covid/Ingestion/germany_vaccinations_timeseries_v2/'
save_path_germany_deliveries_timeseries_v2 = 'mnt/kaggle/Covid/Ingestion/germany_deliveries_timeseries_v2/'
save_path_germany_vaccinations_by_state_v1 = 'mnt/kaggle/Covid/Ingestion/germany_vaccinations_by_state_v1/'

# COMMAND ----------

df = spark.createDataFrame(pd.read_csv(url_germany_vaccinations_timeseries_v2,sep='\t',header=0))

# COMMAND ----------

df.write.format('csv').option('sep', ';').mode('overwrite').save(save_path_germany_vaccinations_timeseries_v2)

# COMMAND ----------

df = spark.createDataFrame(pd.read_csv(url_germany_deliveries_timeseries_v2,sep='\t',header=0))
df.write.format('csv').option('sep', ';').mode('overwrite').save(save_path_germany_deliveries_timeseries_v2)

# COMMAND ----------

df = spark.createDataFrame(pd.read_csv(url_germany_vaccinations_by_state_v1,sep='\t',header=0))
df.write.format('csv').option('sep', ';').mode('overwrite').save(save_path_germany_vaccinations_by_state_v1)

# COMMAND ----------


