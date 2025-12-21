# Databricks notebook source

spark.sql("""%sql
Insert into training_catalog.silver.orders_fact
(
  order_id,
  customer_id,
  order_date,
  amount,
  product_id,
  quantity,
  load_ts
)
(
  select 
  order_id,
  customer_id,
  order_date,
  amount,
  product_id,
  quantity,
  current_timestamp()

  from training_catalog.bronze.orders_stg
  where load_ts > (
    SELECT last_successful_run
    FROM training_catalog.default.control_log_status
    WHERE job_id = 2
)
);
""")


spark.sql("""
update training_catalog.default.control_log_status
set last_successful_run = current_timestamp(),
updated_ts = current_timestamp(),
status = 'SUCCESS'
where job_id = 2;
""")