# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.window import *

%run /Workspace/Users/charansairangasthalam1985@gmail.com/.bundle/databricks-data-platform/default/files/notebooks/lb_utilities

dbutils.widgets.text("file_name", "")
dbutils.widgets.text("bronze_table_name", "")
dbutils.widgets.text("schema_evolution_flag", "true")

file_name = dbutils.widgets.get("file_name")
bronze_table_name = dbutils.widgets.get("bronze_table_name")
schema_evolution_flag = dbutils.widgets.get("schema_evolution_flag")


print(file_name, bronze_table_name, schema_evolution_flag)

if not bronze_table_name or not file_name:
    raise ValueError("file_name or table_name cannot be empty")




landing_path = f"/Volumes/training_catalog/autoloader_demo/landing_volume/{file_name}/"
schema_location = f"/Volumes/training_catalog/autoloader_demo/schema_checkpoint_volume/{file_name}/schema_location/"
checkpoint_location = f"/Volumes/training_catalog/autoloader_demo/schema_checkpoint_volume/{file_name}/checkpoint_location/"
target_bronze_table = f"training_catalog.bronze.{bronze_table_name}"

before_insertions = spark.sql(f"""
                            select count(1) from {target_bronze_table};
                            """)

if schema_evolution_flag.lower() == "true":

    df_bronze_stream  = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format", "csv")\
                        .option("cloudFiles.schemaLocation" , schema_location)\
                        .option("cloudFiles.schemaEvolutionMode", "rescue")\
                            .load(landing_path)

else:

    df_bronze_stream  = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format", "csv")\
                        .option("cloudFiles.schemaLocation" , schema_location)\
                        .option("cloudFiles.schemaEvolutionMode", "fail")\
                            .load(landing_path)

curr_ts = current_timestamp()

df_bronze_stream = add_loadts(df_bronze_stream)

for column in df_bronze_stream.columns:
    if "date" in column:
        df_bronze_stream = df_bronze_stream.withColumn(column, to_date(column))

df_bronze_stream = df_bronze_stream.withColumn("source_file" , input_file_name())

print(target_bronze_table)



df_bronze_stream.writeStream\
    .format("delta")\
    .option("checkpointLocation", checkpoint_location)\
    .trigger(once=True)\
    .toTable(target_bronze_table)

after_insertions = spark.sql(f"""
                            select count(1) from {target_bronze_table};
                            """)

inserted_count = (
    after_insertions.collect()[0][0]
    - before_insertions.collect()[0][0]
)

print(
    f"number of records inserted into bronze table {target_bronze_table} is: {inserted_count}"
)
