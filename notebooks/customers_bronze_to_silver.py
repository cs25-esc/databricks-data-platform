# Databricks notebook source

spark.sql("""
CREATE OR REPLACE TEMP VIEW temp_customer_delta AS
  SELECT *
  FROM training_catalog.bronze.customers_stg
  WHERE load_ts > (
      SELECT last_successful_run
      FROM training_catalog.default.control_log_status
      WHERE job_id = 1
  )
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY load_ts DESC
  ) = 1;""")

spark.sql("""
  MERGE INTO training_catalog.silver.customers_dim tgt
  USING temp_customer_delta src
  ON tgt.customer_id = src.customer_id
  AND tgt.is_current = true
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_current = false,
      tgt.end_ts = current_timestamp();

  INSERT INTO training_catalog.silver.customers_dim
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    true,
    current_timestamp(),
    null
  FROM temp_customer_delta;
""")


spark.sql("""  UPDATE training_catalog.default.control_log_status
  SET
    last_successful_run = current_timestamp(),
    status = 'SUCCESS',
    updated_ts = current_timestamp()
  WHERE job_id = 1;""")

