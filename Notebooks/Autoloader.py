# Databricks notebook source
# dbutils.fs.ls('/mnt/ingest/')

# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": "94a989ad-6007-4d2c-b4f0-d2ea360f3d48",
#           "fs.azure.account.oauth2.client.secret": "09w8Q~GdsmoQvqUIDJZufrcrFwAfBws_kOtexaLa",
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d232b037-d49e-42a4-b8a1-3bec0be5960e/oauth2/token"}

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# dbutils.fs.mount(source = "abfss://curated@satsedataplatformdev.dfs.core.windows.net/",mount_point = "/mnt/curated", extra_configs = configs)                 

# COMMAND ----------

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.types import StructType
import re

# COMMAND ----------

def handle_table_creation(
    data_frame: DataFrame,
    path_table: str,
    db_name: str,
    table_name: str,
    partition_column_names: list = None,
):
    """Convenience function to handle schema and delta table creation at once based on input data frame.

    Args:
        data_frame (DataFrame): _description_
        path_schema (str): path to then schema of the table.
        path_table (str): path to the delta table in the datalake.
        db_name (str): name of the database to store the table.
        table_name (str): name of the delta table.
        partition_column_names (list, optional): names of the partition columns. Defaults to None.
    """
    schema = df.schema

    if not schema.fields:
        schema = new_schema(data_frame=data_frame, schema_file_path=path_schema)
    if not check_delta_table_exists(path_table):
        create_delta_table(
            table_path=path_table,
            schema=schema,
            partition_column_names=partition_column_names,
        )

    register_table_to_hive(db_name=db_name, table_name=table_name, path=path_table)

# COMMAND ----------

def check_delta_table_exists(table_path: str) -> bool:
    """Checks if the delta table exists in a path

    Args:
        table_path (str): path of a table to check for.

    Returns:
        bool: true -> table exists, false -> table does not exist.
    """
    return DeltaTable.isDeltaTable(spark, table_path)

# COMMAND ----------

def create_delta_table(table_path: str, schema: StructType, partition_column_names: list = None):
    """Creates a new delta table.

    Args:
        table_path (spark): path where the delta table should be stored.
        schema (StructType): schema of the table.
        partition_column_names (list): list of column names to partition the delta table.
    """
    schema_cols = ", ".join(
        [f'`{c.name}` {c.dataType.simpleString()}{"" if c.nullable else " NOT NULL"}' for c in schema.fields]
    )

    properties = [
        "delta.autoOptimize.optimizeWrite = true",
        "delta.autoOptimize.autoCompact = true",
        "delta.enableChangeDataFeed = true",
    ]

    # Check if column names contain special characters.
    special_chars_regex = re.compile("[ ,;{}()\n\t=%]")
    contains_special_chars = False
    for column in schema.fields:
        if special_chars_regex.search(column.name):
            contains_special_chars = True
            break

    # In case of special characters in column names use Delta column mapping feature to avoid errors when
    # creating table
    if contains_special_chars:
        properties = properties + [
            "delta.minReaderVersion = 2",
            "delta.minWriterVersion = 5",
            "delta.columnMapping.mode = 'name'",
        ]

    properties_str = f"TBLPROPERTIES ({', '.join(properties)})"

    partition_str = ""

    if partition_column_names:
        partition_cols = filter(
            lambda field: field.name.startswith(tuple(partition_column_names)),
            schema.fields,
        )
        partition_cols = ", ".join([f"{c.name}" for c in list(partition_cols)])
        partition_str = f"PARTITIONED BY ({partition_cols})"

    sql_str = f"""
            CREATE TABLE delta.`{table_path}`
            ({schema_cols})
            USING DELTA
            {partition_str}
            {properties_str}
        """
    spark.sql(sql_str)

# COMMAND ----------

def register_table_to_hive(db_name: str, table_name: str, path: str) -> None:
    """Register a table in Hive Metastore

    Args:
        db_name (str): database name.
        table_name (str): table name.
        path (str): path of the delta table.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"USE {db_name}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db_name}.{table_name} USING DELTA LOCATION '{path}'")

# COMMAND ----------

cloudfile = {
    "cloudFiles.subscriptionId": "aca54848-01e6-4090-9173-a5ead63aaa65",
    "cloudFiles.tenantId": "d232b037-d49e-42a4-b8a1-3bec0be5960e",
    "cloudFiles.clientId": "ee71c1a0-dca4-40ff-aaeb-fb525454ba41",
    "cloudFiles.clientSecret": "7u88Q~oUxKH5PpUTZH6UgyCHkQSVNv9mTHEJfcbv",
    "cloudFiles.resourceGroup": "rg-tse-dataplatform-storage-dev",
    "cloudFiles.useNotifications": "true",
    "cloudFiles.format": "csv",
    "cloudFiles.schemaLocation": "/mnt/ingest/schema/",
    "cloudFiles.schemaEvolutionMode": "rescue",
}

# COMMAND ----------

df = (spark.readStream.format("cloudFiles").options(**cloudfile).option("rescuedDataColumn", "_rescued_data")
.option("multiLine", "true")
.option('header', 'true')
.load("/mnt/ingest/").select("*", "_metadata"))

# COMMAND ----------

handle_table_creation(df, "/mnt/raw", 'raw', 'bip')

# COMMAND ----------

df.writeStream.trigger(availableNow=True).format("delta").option('checkpointLocation', "/mnt/ingest/checkpoint/").outputMode("append").toTable("raw.bip")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM raw.bip

# COMMAND ----------


