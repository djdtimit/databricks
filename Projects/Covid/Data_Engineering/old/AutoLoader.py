# Databricks notebook source
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

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.useNotifications", "true")
      .option("cloudFiles.allowOverwrites", "true")
      .option("cloudFiles.connectionString", connectionString)
      .option("cloudFiles.resourceGroup", resourceGroup)
      .option("cloudFiles.subscriptionId", subscriptionId)
      .option("cloudFiles.tenantId", tenantId)
      .option("cloudFiles.clientId", clientId)
      .option("cloudFiles.clientSecret", clientSecret)
      .option("cloudFiles.schemaLocation", '/mnt/covid/Ingestion/RKI_COVID19/')
      .option("cloudFiles.format", "json")
      .load('/mnt/covid/Ingestion/RKI_COVID19/'))

# COMMAND ----------

(df.writeStream.format("delta") 
  .option("checkpointLocation", '/mnt/covid/Raw/TBL_RKI_COVID19/checkpoint/' )
  .option("cloudFiles.useNotifications", True)
  .trigger(once=True)
  .start('/mnt/covid/Raw/TBL_RKI_COVID19/'))

# COMMAND ----------


