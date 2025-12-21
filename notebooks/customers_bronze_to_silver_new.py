# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.window import *

%run /Workspace/Users/charansairangasthalam1985@gmail.com/.bundle/databricks-data-platform/default/files/notebooks/lb_utilities


df_bronze = spark.sql(""" SELECT *
  FROM training_catalog.bronze.customers_stg
  WHERE load_ts > (
      SELECT last_successful_run
      FROM training_catalog.default.control_log_status
      WHERE job_id = 1
  )
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY load_ts DESC
  ) = 1""")


df_bronze_hash = add_hash_column(df_bronze, ["first_name","last_name", "email", "signup_date"])

df_bronze_hash.createOrReplaceTempView("df_bronze_hash_vw")

display(spark.table('df_bronze_hash_vw').limit(2))


spark.sql("""
MERGE INTO training_catalog.silver.customers_dim tgt
USING df_bronze_hash_vw src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = true

WHEN MATCHED AND tgt.hash_value <> src.hash_value THEN
  UPDATE SET
    tgt.is_current = false,
    tgt.end_ts = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    first_name,
    last_name,
    email,
    hash_value,
    is_current,
    start_ts,
    end_ts
  )
  VALUES (
    src.customer_id,
    src.first_name,
    src.last_name,
    src.email,
    src.hash_value,
    true,
    current_timestamp(),
    null
  );

          """)


spark.sql("""  UPDATE training_catalog.default.control_log_status
  SET
    last_successful_run = current_timestamp(),
    status = 'SUCCESS',
    updated_ts = current_timestamp()
  WHERE job_id = 1;""")
