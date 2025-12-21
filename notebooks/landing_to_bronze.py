# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.window import *

%run /Workspace/Users/charansairangasthalam1985@gmail.com/.bundle/databricks-data-platform/default/files/notebooks/lb_utilities

file_name = dbutils.widgets.get("file_name")
bronze_table_name = dbutils.widgets.get("bronze_table_name")


print(file_name, bronze_table_name)



dbutils.widgets.text("file_name", "")
dbutils.widgets.text("bronze_table_name", "")



landing_path = f"/Volumes/training_catalog/autoloader_demo/landing_volume/{file_name}/"
schema_location = f"/Volumes/training_catalog/autoloader_demo/schema_checkpoint_volume/{file_name}/schema_location/"
checkpoint_location = f"/Volumes/training_catalog/autoloader_demo/schema_checkpoint_volume/{file_name}/checkpoint_location/"
target_bronze_table = f"training_catalog.bronze.{bronze_table_name}"

df_customers_stream  = spark.readStream.format("cloudFiles")\
                    .option("cloudFiles.format", "csv")\
                      .option("cloudFiles.schemaLocation" , schema_location)\
                      .option("cloudFiles.schemaEvolutionMode", "rescue")\
                          .load(landing_path)

df_customers_stream = add_loadts(df_customers_stream)

for column in df_customers_stream.columns:
    if "date" in column:
        df_customers_stream = df_customers_stream.withColumn(column, to_date(column))


df_customers_stream = add_loadts(df_customers_stream)

print(target_bronze_table)

df_customers_stream.writeStream\
    .format("delta")\
    .option("checkpointLocation", checkpoint_location)\
    .trigger(once=True)\
    .toTable(target_bronze_table)
