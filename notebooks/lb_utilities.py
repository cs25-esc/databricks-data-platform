from pyspark.sql.functions import *

def add_loadts(df):
    return df.withColumn("load_ts", current_timestamp())