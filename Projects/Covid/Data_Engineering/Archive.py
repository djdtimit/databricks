# Databricks notebook source
database_objects = spark.sql("show tables in covid_raw").select('database', 'tableName').collect()
for database_object in database_objects:
  database_name = database_object['database']
  table_name = database_object['tableName']
  paths = spark.sql(f"""SELECT distinct _source as PATH FROM {database_name}.{table_name}""").collect()
  source_mnt_path = paths[0]['PATH'].rsplit('/',1)[0]
  
  listed_files_paths = [listed_file[0] for listed_file in dbutils.fs.ls(source_mnt_path)]
    
  for path in paths:
    source_path = path['PATH']
    target_path = path['PATH'].replace('/Ingestion/', '/Archive/')
    
    # check if files in data lake do exist
    if source_path in listed_files_paths:
      dbutils.fs.mv(source_path, target_path)
    
