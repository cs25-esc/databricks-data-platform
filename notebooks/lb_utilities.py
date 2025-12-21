from pyspark.sql.functions import *

def add_loadts(df):
    return df.withColumn("load_ts", current_timestamp())


def add_hash_column(df, col_list):
    df = df.withColumn("hash_value" , sha2(concat_ws("||" , *[col(c) for c in col_list]), 0))

    return df