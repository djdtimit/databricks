-- Databricks notebook source
CREATE TABLE if NOT EXISTS raw.dbt_audits (
id bigint GENERATED ALWAYS AS IDENTITY  ( START WITH 1  INCREMENT BY 1  ) ,
audit_type varchar(50),
created_at timestamp GENERATED ALWAYS as (now())
)

-- COMMAND ----------


